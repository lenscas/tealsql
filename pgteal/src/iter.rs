use mlua::IntoLuaMulti;
use std::{
    collections::VecDeque,
    sync::{
        mpsc::{self, TryRecvError},
        Arc, Mutex, MutexGuard,
    },
    thread::JoinHandle,
};
use tealr::{
    mlu::mlua::{FromLua, IntoLua, UserData, UserDataRef, UserDataRefMut},
    RecordGenerator, TealMultiValue, ToTypename,
};
use tealr::{
    mlu::{TealData, UserDataWrapper},
    TypeBody,
};

use sqlx::postgres::PgRow;

use std::sync::mpsc::{Receiver, Sender};

use crate::base::Error;

struct ReceiverAndCache(VecDeque<AsyncMessage>, Receiver<Vec<AsyncMessage>>);

pub(crate) enum AsyncMessage {
    Value(PgRow),
    Error(sqlx::Error),
    DynError(Arc<dyn std::error::Error + Sync + Send>),
}
pub(crate) struct Iter<X> {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    channel: Arc<Mutex<ReceiverAndCache>>,
    _x: std::marker::PhantomData<fn() -> X>,
}

impl<X> Clone for Iter<X> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            channel: self.channel.clone(),
            _x: self._x,
        }
    }
}
impl<X: ToTypename> tealr::ToTypename for Iter<X> {
    fn to_typename() -> tealr::Type {
        tealr::Type::new_single_with_generics(
            "Stream",
            tealr::KindOfType::External,
            vec![X::to_typename()],
        )
    }
}

impl<X: ToTypename + 'static + FromLua + IntoLua> TypeBody for Iter<X> {
    fn get_type_body() -> tealr::TypeGenerator {
        let mut a = RecordGenerator::new::<Self>(false);
        a.is_user_data = true;
        <Self as TealData>::add_fields(&mut a);
        <Self as TealData>::add_methods(&mut a);
        tealr::TypeGenerator::Record(Box::new(a))
    }
}

impl<X: ToTypename + 'static + FromLua + IntoLua> UserData for Iter<X> {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        <Self as TealData>::add_fields(&mut UserDataWrapper::from_user_data_fields(fields));
    }

    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        <Self as TealData>::add_methods(&mut UserDataWrapper::from_user_data_methods(methods));
    }
}

fn get_through_locs(
    lock_channel: &mut MutexGuard<ReceiverAndCache>,
    force: bool,
) -> Result<(Option<PgRow>, bool), tealr::mlu::mlua::Error> {
    loop {
        let is_disconnected = if lock_channel.0.is_empty() {
            match lock_channel.1.try_recv() {
                Err(TryRecvError::Disconnected) => true,
                Err(TryRecvError::Empty) => false,
                Ok(x) => {
                    lock_channel.0.extend(x);
                    false
                }
            }
        } else {
            false
        };

        if is_disconnected {
            break Ok((None, true));
        } else {
            let item = lock_channel.0.pop_front();
            let res = match item {
                Some(AsyncMessage::DynError(x)) => {
                    return Err(tealr::mlu::mlua::Error::external(x))
                }
                Some(AsyncMessage::Error(x)) => return Err(Error::Sqlx(x).into()),
                Some(AsyncMessage::Value(x)) => return Ok((Some(x), false)),
                None => None,
            };
            if !force {
                break Ok((res, false));
            }
        }
    }
}

tealr::mlu::create_generic!(pub(crate) Out);

impl<X: ToTypename + 'static + mlua::FromLua + IntoLuaMulti + TealMultiValue> Iter<X> {
    pub(crate) fn from_func<
        ThreadFunc: FnOnce() + Send + 'static,
        FuncSpawner: FnOnce(Sender<Vec<AsyncMessage>>) -> ThreadFunc,
    >(
        func: FuncSpawner,
    ) -> Self {
        let (sender, rec) = mpsc::channel();
        let thread_func = func(sender);
        let handle = std::thread::spawn(thread_func);
        Self::new(handle, rec)
    }

    pub(crate) fn new(handle: JoinHandle<()>, channel: Receiver<Vec<AsyncMessage>>) -> Self {
        Self {
            handle: Arc::new(Mutex::new(Some(handle))),
            channel: Arc::new(Mutex::new(ReceiverAndCache(Default::default(), channel))),
            _x: std::marker::PhantomData,
        }
    }

    fn join(&mut self) {
        match self.handle.lock() {
            Ok(mut x) => {
                if let Some(x) = x.take() {
                    let _ = x.join();
                }
            }
            Err(_) => todo!(),
        }
    }

    fn get_lock(
        a: &mut Arc<Mutex<ReceiverAndCache>>,
    ) -> Result<MutexGuard<'_, ReceiverAndCache>, tealr::mlu::mlua::Error> {
        match a.lock() {
            Ok(x) => Ok(x),
            Err(_) => Err(tealr::mlu::mlua::Error::external(
                crate::base::Error::Custom("channel is already in use".into()),
            )),
        }
    }

    fn get_from_cache(&mut self, force: bool) -> Result<Option<PgRow>, tealr::mlu::mlua::Error> {
        let (item, is_disconnected) = {
            let mut lock_channel = Self::get_lock(&mut self.channel)?;
            get_through_locs(&mut lock_channel, force)?
        };
        if is_disconnected {
            self.join();
        }
        Ok(item)
    }

    fn run_all(
        &mut self,
        force: bool,
        lua: &tealr::mlu::mlua::Lua,
        func: tealr::mlu::TypedFunction<X, Out>,
    ) -> Result<Vec<Out>, tealr::mlu::mlua::Error> {
        let mut res = Vec::new();
        {
            let mut lock_channel = Self::get_lock(&mut self.channel)?;
            loop {
                let (item, is_disconnected) = get_through_locs(&mut lock_channel, force)?;
                if let Some(x) = item {
                    let x = X::from_lua(crate::pg_row::LuaRow::from(x).into_lua(lua)?, lua)?;

                    res.push(func.call(x)?);
                }

                if is_disconnected {
                    break;
                }
            }
        }
        self.join();
        Ok(res)
    }

    fn next_lua_maybe_cached(
        &mut self,
        lua: &tealr::mlu::mlua::Lua,
        force: bool,
        cached: Option<tealr::mlu::mlua::Table>,
    ) -> tealr::mlu::mlua::Result<Option<X>> {
        let cached = match cached {
            Some(x) => x,
            None => lua.create_table()?,
        };
        self.next_lua(lua, force, cached)
    }

    fn next_lua(
        &mut self,
        lua: &tealr::mlu::mlua::Lua,
        force: bool,
        cached: tealr::mlu::mlua::Table,
    ) -> tealr::mlu::mlua::Result<Option<X>> {
        let next = self
            .get_from_cache(force)?
            .map(|v| crate::pg_row::LuaRow::from(v).into_lua_cached(lua, cached));
        match next {
            Some(Err(x)) => Err(x),
            Some(Ok(x)) => Ok(Some(X::from_lua(x, lua)?)),
            None => Ok(None),
        }
    }
}

impl<X: ToTypename + 'static + mlua::FromLua + mlua::IntoLua> TealData for Iter<X> {
    fn add_methods<T: tealr::mlu::TealDataMethods<Self>>(methods: &mut T) {
        methods.document_type("Returned from connection:fetch_all_async(). It allows you to do other things while the query is running in a background thread.");

        methods.document("returns the next item if it is available or nill if not.");
        methods.document("Does NOT block the main thread.");
        methods.add_method_mut("try_next", |lua, this, ()| {
            let item = this.next_lua_maybe_cached(lua, false, None)?;
            Ok((item.is_some(), item))
        });
        methods.document("Waits until the next item is available and then returns it.");
        methods.document("DOES block the main thread");
        methods.add_method_mut("next", |lua, this, ()| {
            this.next_lua_maybe_cached(lua, true, None)
        });
        methods.document("Constructs a blocking iterator that will loop over all the items.");
        methods.add_method("iter", |lua, this, ()| {
            let func = tealr::mlu::TypedFunction::from_rust_mut(
                move |lua, mut this: UserDataRefMut<Self>| {
                    this.next_lua_maybe_cached(lua, true, None)
                },
                lua,
            )?;
            Ok((func, this.to_owned()))
        });
        methods.add_method_mut(
            "loop_all",
            |lua, this, func: tealr::mlu::TypedFunction<X, Out>| this.run_all(true, lua, func),
        );
        methods.generate_help();
    }
}

impl<X: ToTypename + 'static> FromLua for Iter<X> {
    fn from_lua(
        value: tealr::mlu::mlua::Value,
        lua: &tealr::mlu::mlua::Lua,
    ) -> tealr::mlu::mlua::Result<Self> {
        let x = UserDataRef::<Self>::from_lua(value, lua)?;
        Ok(Iter::to_owned(&x))
    }
}

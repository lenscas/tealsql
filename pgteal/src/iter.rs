use mlua::ToLua;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc, Mutex, MutexGuard},
    thread::JoinHandle,
};
use tealr::{mlu::TealData, new_type, NamePart};

use sqlx::postgres::PgRow;

use triple_buffer::{Input, Output, TripleBuffer};

use crate::base::Error;

pub(crate) enum AsyncMessage {
    Value(PgRow),
    Error(sqlx::Error),
    DynError(Arc<dyn std::error::Error + Sync + Send>),
}
#[derive(tealr::mlu::UserData, Clone)]
pub(crate) struct Iter {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    channel: Arc<Mutex<Output<VecDeque<AsyncMessage>>>>,
    close_check: Arc<AtomicBool>,
}

impl tealr::TypeName for Iter {
    fn get_type_parts() -> std::borrow::Cow<'static, [NamePart]> {
        new_type!(Stream)
    }
}

fn get_through_locs(
    close_check: Arc<AtomicBool>,
    lock_channel: &mut MutexGuard<Output<VecDeque<AsyncMessage>>>,
    force: bool,
) -> Result<(Option<PgRow>, bool), mlua::Error> {
    loop {
        let x = lock_channel.output_buffer();
        let disconnected = x.is_empty() && close_check.load(std::sync::atomic::Ordering::SeqCst);
        if disconnected {
            break Ok((None, true));
        } else {
            let item = x.pop_front();
            lock_channel.update();
            let res = match item {
                Some(AsyncMessage::DynError(x)) => return Err(mlua::Error::external(x)),
                Some(AsyncMessage::Error(x)) => return Err(Error::Sqlx(x).into()),
                Some(AsyncMessage::Value(x)) => Some(x),
                None => None,
            };
            if !force {
                break Ok((res, false));
            }
        }
    }
}

impl Iter {
    pub(crate) fn from_func<
        ThreadFunc: FnOnce() + Send + 'static,
        FuncSpawner: FnOnce(Input<VecDeque<AsyncMessage>>, Arc<AtomicBool>) -> ThreadFunc,
    >(
        func: FuncSpawner,
    ) -> Self {
        let close_check = Arc::new(AtomicBool::new(false));
        let (sender, rec) = TripleBuffer::default().split();
        let thread_func = func(sender, close_check.clone());
        let handle = std::thread::spawn(thread_func);
        Self::new(handle, rec, close_check)
    }

    pub(crate) fn new(
        handle: JoinHandle<()>,
        channel: Output<VecDeque<AsyncMessage>>,
        close_check: Arc<AtomicBool>,
    ) -> Self {
        Self {
            handle: Arc::new(Mutex::new(Some(handle))),
            channel: Arc::new(Mutex::new(channel)),
            close_check,
        }
    }

    fn join(&mut self) {
        self.close_check
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
        a: &mut Arc<Mutex<Output<VecDeque<AsyncMessage>>>>,
    ) -> Result<MutexGuard<'_, Output<VecDeque<AsyncMessage>>>, mlua::Error> {
        match a.lock() {
            Ok(x) => Ok(x),
            Err(_) => Err(mlua::Error::external(crate::base::Error::Custom(
                "channel is already in use".into(),
            ))),
        }
    }

    fn get_from_cache(&mut self, force: bool) -> Result<Option<PgRow>, mlua::Error> {
        let (item, is_disconnected) = {
            let mut lock_channel = Self::get_lock(&mut self.channel)?;
            get_through_locs(self.close_check.clone(), &mut lock_channel, force)?
        };
        if is_disconnected {
            self.join();
        }
        Ok(item)
    }

    fn run_all<'lua>(
        &mut self,
        force: bool,
        lua: &'lua mlua::Lua,
        func: tealr::mlu::TypedFunction<'lua, mlua::Value<'lua>, tealr::mlu::generics::X<'lua>>,
    ) -> Result<Vec<tealr::mlu::generics::X<'lua>>, mlua::Error> {
        let mut res = Vec::new();
        {
            let mut lock_channel = Self::get_lock(&mut self.channel)?;
            loop {
                let (item, is_disconnected) =
                    get_through_locs(self.close_check.clone(), &mut lock_channel, force)?;
                if let Some(x) = item {
                    let x = crate::pg_row::LuaRow::from(x).to_lua(lua)?;
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

    fn next_lua_maybe_cached<'lua>(
        &mut self,
        lua: &'lua mlua::Lua,
        force: bool,
        cached: Option<mlua::Table<'lua>>,
    ) -> mlua::Result<Option<mlua::Value<'lua>>> {
        let cached = match cached {
            Some(x) => x,
            None => lua.create_table()?,
        };
        self.next_lua(lua, force, cached)
    }

    fn next_lua<'lua>(
        &mut self,
        lua: &'lua mlua::Lua,
        force: bool,
        cached: mlua::Table<'lua>,
    ) -> mlua::Result<Option<mlua::Value<'lua>>> {
        let next = self
            .get_from_cache(force)?
            .map(|v| crate::pg_row::LuaRow::from(v).into_lua_cached(lua, cached));
        match next {
            Some(Err(x)) => Err(x),
            Some(Ok(x)) => Ok(Some(x)),
            None => Ok(None),
        }
    }
}

impl TealData for Iter {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
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
        methods.add_function("iter", |lua, this: Self| {
            let func = tealr::mlu::TypedFunction::from_rust_mut(
                |lua, mut this: Self| this.next_lua_maybe_cached(lua, true, None),
                lua,
            )?;
            Ok((func, this))
        });
        methods.add_method_mut("loop_all", |lua,this, func:tealr::mlu::TypedFunction<tealr::mlu::mlua::Value, tealr::mlu::generics::X>|{
            this.run_all(true,lua,func)
        });
        methods.generate_help();
    }
}

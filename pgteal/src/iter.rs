use mlua::{FromLua, ToLua};
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc, Mutex},
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
#[derive(Clone, tealr::MluaUserData)]
pub(crate) struct Iter {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    channel: Arc<Mutex<Output<VecDeque<AsyncMessage>>>>,
    close_check: Arc<AtomicBool>,
}

impl<'e> tealr::TypeName for Iter {
    fn get_type_parts(_: tealr::Direction) -> std::borrow::Cow<'static, [NamePart]> {
        new_type!(Stream)
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

    fn get_from_cache(&mut self, force: bool) -> Result<Option<PgRow>, mlua::Error> {
        let (item, is_disconnected) = loop {
            let mut lock_channel = match self.channel.lock() {
                Ok(x) => x,
                Err(_) => {
                    return Err(mlua::Error::external(crate::base::Error::Custom(
                        "channel is already in use".into(),
                    )))
                }
            };
            let x = lock_channel.output_buffer();
            let disconnected =
                x.is_empty() && self.close_check.load(std::sync::atomic::Ordering::SeqCst);
            if disconnected {
                break (None, true);
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
                    break (res, false);
                }
            }
        };
        if is_disconnected {
            self.join();
        }
        Ok(item)
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
            this.next_lua_maybe_cached(lua, false, None)
        });
        methods.document("Waits until the next item is available and then returns it.");
        methods.document("DOES block the main thread");
        methods.add_method_mut("next", |lua, this, ()| {
            this.next_lua_maybe_cached(lua, true, None)
        });
        methods.document("Constructs a blocking iterator that will loop over all the items.");
        methods.add_method("iter", |lua, this, ()| {
            let mut this = this.to_owned();
            let x = lua.create_function_mut(move |lua, table| this.next_lua(lua, true, table))?;
            let x = x.to_lua(lua)?;
            let x = tealr::mlu::TypedFunction::<Self, mlua::Value>::from_lua(x, lua)?;
            Ok((x, lua.create_table()?))
        });
        methods.generate_help();
    }
}

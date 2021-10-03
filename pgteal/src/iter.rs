use mlua::{FromLua, ToLua};
use std::{
    collections::VecDeque,
    sync::{
        mpsc::{Receiver, RecvError, Sender, TryRecvError},
        Arc, Mutex,
    },
    thread::JoinHandle,
};
use tealr::mlu::TealData;

use sqlx::postgres::PgRow;

// use triple_buffer::{Input, Output, TripleBuffer};

pub(crate) enum AsyncMessage {
    Value(PgRow),
    Error(sqlx::Error),
    DynError(Arc<dyn std::error::Error + Sync + Send>),
}
#[derive(Clone, tealr::MluaUserData)]
pub(crate) struct Iter {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    channel: Arc<Mutex<Receiver<Vec<AsyncMessage>>>>,
    cache: Arc<Mutex<VecDeque<PgRow>>>,
}

impl<'e> tealr::TypeName for Iter {
    fn get_type_name(_: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Stream<any>")
    }
}

impl Iter {
    pub(crate) fn from_func<
        ThreadFunc: FnOnce() + Send + 'static,
        FuncSpawner: FnOnce(Sender<Vec<AsyncMessage>>) -> ThreadFunc,
    >(
        func: FuncSpawner,
    ) -> Self {
        let (sender, rec) = std::sync::mpsc::channel();
        let thread_func = func(sender);
        let handle = std::thread::spawn(thread_func);
        Self::new(handle, rec)
    }

    pub(crate) fn new(handle: JoinHandle<()>, channel: Receiver<Vec<AsyncMessage>>) -> Self {
        Self {
            handle: Arc::new(Mutex::new(Some(handle))),
            channel: Arc::new(Mutex::new(channel)),
            cache: Arc::new(Mutex::new(VecDeque::new())),
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

    fn get_from_cache(&mut self, force: bool) -> Result<Option<PgRow>, mlua::Error> {
        let (item, is_disconnected) = {
            let mut lock_cache = match self.cache.lock() {
                Ok(x) => x,
                Err(_) => {
                    return Err(mlua::Error::external(crate::base::Error::Custom(
                        "cache is already in use".into(),
                    )))
                }
            };
            let lock_channel = match self.channel.lock() {
                Ok(x) => x,
                Err(_) => {
                    return Err(mlua::Error::external(crate::base::Error::Custom(
                        "channel is already in use".into(),
                    )))
                }
            };
            let disconnected = if lock_cache.is_empty() {
                let result = if force {
                    lock_channel.recv()
                } else {
                    match lock_channel.try_recv() {
                        Err(TryRecvError::Empty) => Ok(Vec::new()),
                        Err(TryRecvError::Disconnected) => Err(RecvError),
                        Ok(x) => Ok(x),
                    }
                };
                match result {
                    Err(_) => true,
                    Ok(result) => {
                        for result in result {
                            match result {
                                AsyncMessage::Value(x) => {
                                    lock_cache.push_back(x);
                                }
                                AsyncMessage::Error(x) => return Err(mlua::Error::external(x)),
                                AsyncMessage::DynError(x) => return Err(mlua::Error::external(x)),
                            }
                        }
                        false
                    }
                }
            } else {
                false
            };
            (lock_cache.pop_front(), disconnected)
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
        methods
            .document("returns the next item if it is available. Does NOT block the main thread.");
        methods.add_method_mut("try_next", |lua, this, ()| {
            this.next_lua_maybe_cached(lua, false, None)
        });
        methods.document("Waits until the next item is available and then returns it. DOES block the main thread");
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

use async_std::task::block_on;
use mlua::{FromLua, ToLua};
use std::{
    pin::Pin,
    sync::{mpsc::Receiver, Arc, Mutex},
    thread::JoinHandle,
};
use tealr::mlu::TealData;

use futures::{Stream, StreamExt};
use sqlx::{postgres::PgRow, Error};

use crate::pg_row::LuaRow;

pub(crate) enum AsyncMessage {
    Value(PgRow),
    Error(sqlx::Error),
    DynError(Arc<dyn std::error::Error + Sync + Send>),
}
#[derive(Clone, tealr::MluaUserData)]
pub(crate) struct Iter {
    handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    channel: Arc<Mutex<std::sync::mpsc::Receiver<AsyncMessage>>>,
}

impl<'e> tealr::TypeName for Iter {
    fn get_type_name(_: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Stream<any>")
    }
}

impl Iter {
    pub(crate) fn new(handle: JoinHandle<()>, channel: Receiver<AsyncMessage>) -> Self {
        Self {
            handle: Arc::new(Mutex::new(Some(handle))),
            channel: Arc::new(Mutex::new(channel)),
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

    fn next(&mut self) -> Option<PgRow> {
        let res = {
            let channel = match self.channel.lock() {
                Ok(x) => x,
                Err(_) => return None,
            };
            channel.recv()
        };
        match res {
            Ok(AsyncMessage::Value(x)) => Some(x),
            Err(_) => {
                self.join();
                None
            }
            _ => self.next(),
        }
    }

    fn next_lua<'lua>(&mut self, lua: &'lua mlua::Lua) -> mlua::Result<Option<mlua::Value<'lua>>> {
        let next = self
            .next()
            .map(|v| crate::pg_row::LuaRow::from(v).to_lua(lua));
        match next {
            Some(Err(x)) => Err(x),
            Some(Ok(x)) => Ok(Some(x)),
            None => Ok(None),
        }
    }

    fn try_next(&mut self) -> Option<PgRow> {
        let res = {
            let channel = match self.channel.lock() {
                Ok(x) => x,
                Err(_) => return None,
            };
            channel.try_recv()
        };
        match res {
            Ok(AsyncMessage::Value(x)) => Some(x),
            Ok(AsyncMessage::Error(x)) => None,
            Ok(AsyncMessage::DynError(x)) => None,
            Err(std::sync::mpsc::TryRecvError::Empty) => None,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.join();
                None
            }
        }
    }
}

impl TealData for Iter {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method_mut("try_next", |lua, this, ()| {
            let next = this
                .try_next()
                .map(|v| crate::pg_row::LuaRow::from(v).to_lua(lua));
            match next {
                Some(Err(x)) => Err(x),
                Some(Ok(x)) => Ok(Some(x)),
                None => Ok(None),
            }
        });
        methods.add_method_mut("next", |lua, this, ()| this.next_lua(lua));
        methods.add_method("iter", |lua, this, ()| {
            let mut this = this.to_owned();
            let x = lua.create_function_mut(move |lua, ()| this.next_lua(lua))?;
            let x = x.to_lua(lua)?;
            let x = tealr::mlu::TypedFunction::<Self, mlua::Value>::from_lua(x, lua)?;
            Ok((x, lua.create_table()?))
        })
    }
}

// impl<'e> TealData for Iter<'e> {
// fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
// methods.add_method("next", |lua, this, _: ()| this.next(lua));
// methods.add_meta_method(mlua::MetaMethod::Pairs, |lua, this, _: ()| this.next(lua))
// }
// }

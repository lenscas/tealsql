use async_std::task::block_on;
use mlua::ToLua;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tealr::{
    mlu::{self, TealData},
    TypeName,
};

use futures::{Stream, StreamExt};
use sqlx::{postgres::PgRow, Error};

use crate::pg_row::LuaRow;

#[derive(Clone)]
pub(crate) struct Iter<'e> {
    x: Arc<Mutex<Pin<Box<dyn Stream<Item = Result<PgRow, Error>> + 'e + Send>>>>,
    _x: &'e std::marker::PhantomData<()>,
}

impl<'e> Iter<'e> {
    pub(crate) fn new(x: Pin<Box<dyn Stream<Item = Result<PgRow, Error>> + 'e + Send>>) -> Self {
        Self {
            x: Arc::new(Mutex::new(x)),
            _x: &std::marker::PhantomData,
        }
    }
    fn next<'lua>(&self, lua: &'lua mlua::Lua) -> Result<Option<mlua::Value<'lua>>, mlua::Error> {
        let x = self.x.lock();
        let mut x = match x {
            Ok(x) => x,
            Err(_) => {
                return Err(mlua::Error::external(crate::base::Error::Custom(
                    "Lock got poisoned".into(),
                )))
            }
        };
        let res = block_on(x.next());
        let res = res.map(|v| {
            v.map_err(mlua::Error::external)
                .and_then(|v| LuaRow::from(v).to_lua(lua))
        });
        match res {
            None => Ok(None),
            Some(Ok(x)) => Ok(Some(x)),
            Some(Err(x)) => Err(x),
        }
    }
}

impl<'e> tealr::TypeName for Iter<'e> {
    fn get_type_name(dir: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Iter")
    }
}

impl<'e> mlua::UserData for Iter<'e> {}

impl<'e> TealData for Iter<'e> {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method("next", |lua, this, _: ()| this.next(lua));
        methods.add_meta_method(mlua::MetaMethod::Pairs, |lua, this, _: ()| this.next(lua))
    }
}

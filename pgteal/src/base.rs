use std::fmt::Display;

use async_std::task::block_on;
use mlua::{LuaSerdeExt, Value::Nil};
use sqlx::{Connection, PgPool};
use tealr::{
    mlu::{mlua, TealData},
    TypeName,
};

use crate::{connection::LuaConnection, Res};

#[derive(Debug)]
pub(crate) enum Error {
    Sqlx(sqlx::Error),
    Custom(String),
    DBErrorAfterHandling(sqlx::Error, mlua::Error),
}

impl std::error::Error for Error {}

impl From<Error> for mlua::Error {
    fn from(x: Error) -> Self {
        mlua::Error::external(x)
    }
}

impl From<sqlx::Error> for Error {
    fn from(x: sqlx::Error) -> Self {
        Error::Sqlx(x)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Sqlx(x) => x.fmt(f),
            Error::Custom(x) => x.fmt(f),
            Error::DBErrorAfterHandling(x, y) => {
                write!(f, "DB Error:\n{}\n got thrown while handling:\n{}", x, y)
            }
        }
    }
}

#[derive(Clone, tealr::MluaUserData, TypeName)]
pub struct Base {}

impl TealData for Base {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_function("connect_pool", |_, connection_string: String| {
            let res = async {
                let pool = PgPool::connect(&connection_string).await?;
                Ok(crate::pool::Pool::from(pool))
            };
            let res: Result<_, Error> = block_on(res);
            Ok(res?)
        });
        methods.add_function("connect", |_,(connection_string, func): (String,tealr::mlu::TypedFunction<LuaConnection,Res>)| {
            let con = async {
                sqlx::postgres::PgConnection::connect(&connection_string).await.map(LuaConnection::from)
            };
            let con = block_on(con).map_err(mlua::Error::external)?;
            let res =func.call(con.clone());
            con.drop_con()?;
            res
        });
        methods.add_function("nul", |lua, ()| Ok(lua.null()));
        methods.add_meta_function(mlua::MetaMethod::Index, |lua, string: String| {
            if string == "null" {
                Ok(lua.null())
            } else {
                Ok(Nil)
            }
        })
    }
}

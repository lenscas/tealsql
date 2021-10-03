use std::fmt::Display;

use async_std::task::block_on;
use mlua::{LuaSerdeExt, Value::Nil};
use sqlx::{postgres::types::PgInterval, Connection, PgPool};
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
        methods.document("Connect to the server and create a connection pool");
        methods.document("Params:");
        methods.document("connection_string:The string used to connect to the server.");
        methods.add_function("connect_pool", |_, connection_string: String| block_on(async move {
            let pool = PgPool::connect(&connection_string)
                .await
                .map_err(Error::from)?;
            Ok(crate::pool::Pool::from(pool))
        }));
        methods.document("Connect to the server and create a single connection");
        methods.document("Params:");
        methods.document("connection_string:The string used to connect to the server.");
        methods.document(
            "func: The function that will be executed after the connection has been made.",
        );
        methods.document("This function receives the connection object, which will be cleaned up after the function has been executed.");
        methods.document(
            "A value returned from this function will also be returned by the connect function",
        );
        methods.add_function("connect", |_,(connection_string, func): (String,tealr::mlu::TypedFunction<LuaConnection,Res>)| block_on(async move {
            let con = 
                sqlx::postgres::PgConnection::connect(&connection_string).await.map(LuaConnection::from).map_err(Error::from)?;
            let res =func.call(con.clone());
            con.drop_con()?;
            res
        }));
        methods.document("Returns the value used to represent `null` values in json.");
        methods.add_function("nul", |lua, ()| Ok(lua.null()));
        methods.document("You can index this type with \"null\" to get the value back that is used to represent null in json.");
        methods.add_meta_function(mlua::MetaMethod::Index, |lua, string: String| {
            if string == "null" {
                Ok(lua.null())
            } else {
                Ok(Nil)
            }
        });
        methods.document("Creates the interval type from postgresql.");
        methods.document("Params:");
        methods.document("months: The amount of months in this interval. Defaults to 0");
        methods.document("days: The amount of days in this interval. Defaults to 0");
        methods
            .document("microseconds: The amount of microseconds in this interval. Defaults to 0");
        methods.add_function(
            "interval",
            |_, (months, days, microseconds): (Option<i32>, Option<i32>, Option<i64>)| {
                Ok(shared::Interval::from(PgInterval {
                    months: months.unwrap_or_default(),
                    days: days.unwrap_or_default(),
                    microseconds: microseconds.unwrap_or_default(),
                }))
            },
        );
        methods.generate_help();
    }
}

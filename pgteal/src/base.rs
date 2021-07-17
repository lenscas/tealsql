use std::fmt::Display;

use async_std::task::block_on;
use sqlx::PgPool;
use tealr::{mlu::TealData, TypeName};

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
pub(crate) struct Base {}

impl TealData for Base {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_function("connectPool", |_, connection_string: String| {
            let res = async {
                let pool = PgPool::connect(&connection_string).await?;
                Ok(crate::pool::Pool::from(pool))
            };
            let res: Result<_, Error> = block_on(res);
            Ok(res?)
        })
    }
}

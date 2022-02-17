use std::{fmt::Display, sync::Arc};

use mlua::{LuaSerdeExt, Value::Nil};
use sqlx::{postgres::types::PgInterval, Connection, PgPool};
use tealr::{
    mlu::{mlua, TealData},
    TypeName,
};
use tokio::runtime::Runtime;

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
        methods.document_type("Tealsql is a sql library made to be easy and safe to use. Its RAII centric API prevents mistakes like forgetting to close connections.
The library also makes prepared statements easy to use as it does the binding of parameters for you.");
        methods.document_type("There are also several helper functions to do basic tasks like deleting, updating or inserting values. These allow you to quickly get something going without the need to write SQL for these basic operation");

        methods.document_type("");
        methods.document_type("Lastly, this library is made with teal in mind. This means that a automatically generated `.d.tl` file is shipped with the dependency. Allowing teal users to always have correct type information about this library.");
        methods.document_type("Further more: This library also has a CLI that acts similar to pgtyped but for teal. This gives teal users the ability to write totally type safe sql queries.");
        methods.document("Connect to the server and create a connection pool");
        methods.document("## Params:");
        methods.document("- connection_string:The string used to connect to the server.");
        methods.document("## Example:");
        methods.document(
            "```teal_lua
        local tealsql = require\"libpgteal\"
        local pool = tealsql.connect_pool(\"postgres://userName:password@host/database\")
        local res = pool:get_connection(function(con:libpgteal.Connection):{string:integer})
            return con:fetch_one(\"SELECT $1 as test\",{2}) as {string:integer}
        end)
        assert(res.test ==  2)
        ```\n",
        );
        methods.add_function("connect_pool", |_, connection_string: String| {
            let runtime = Arc::new(Runtime::new()?);
            runtime.clone().block_on(async move {
                let pool = PgPool::connect(&connection_string)
                    .await
                    .map_err(Error::from)?;
                Ok(crate::pool::Pool::new(pool, runtime))
            })
        });
        methods.document("Returns the `.d.tl` file of this library.");
        methods.add_function("gen_defs", |_, ()| {
            crate::generate_defs().map_err(mlua::Error::external)
        });
        methods.document("Returns a json string representing the definitions of this library");
        methods.document("This can be used to generate online documentation");
        methods.document("## Params:");
        methods.document("- pretty: If the json needs to be pretty printed or not");
        methods.add_function("gen_json", |_, pretty: bool| {
            crate::generate_json(pretty).map_err(mlua::Error::external)
        });
        methods.document("Connect to the server and create a single connection");
        methods.document("## Params:");
        methods.document("- connection_string: The string used to connect to the server.");
        methods.document(
            "- func: The function that will be executed after the connection has been made.",
        );
        methods.document("This function receives the connection object, which will be cleaned up after the function has been executed.");
        methods.document(
            "A value returned from this function will also be returned by the connect function",
        );
        methods.document("## Example:");
        methods.document(
            "```teal_lua
local tealsql = require\"libpgteal\"
local res = tealsql.connect(\"postgres://userName:password@host/database\",function(con:tealsql.Connection):{string:integer}
    return con:fetch_one(\"SELECT $1 as test\",{2}) as {string:integer}
end)
assert(res.test ==  2)
```\n",
        );
        methods.add_function("connect", |_,(connection_string, func): (String,tealr::mlu::TypedFunction<LuaConnection,Res>)| {
            let runtime = Arc::new(Runtime::new()?);
            let con = runtime.clone().block_on(async move {
                sqlx::postgres::PgConnection::connect(&connection_string)
                    .await
                    .map(|v|LuaConnection::new(v, runtime))
                    .map_err(Error::from)
        })?;
        let res =func.call(con.clone());
        con.drop_con()?;
        res
        });
        methods.document("Returns the value used to represent `null` values in json.");
        methods.add_function("nul", |lua, ()| Ok(lua.null()));
        methods.document("You can index this type with `\"null\"` to get the value back that is used to represent null in json.");
        methods.add_meta_function(mlua::MetaMethod::Index, |lua, string: String| {
            if string == "null" {
                Ok(lua.null())
            } else {
                Ok(Nil)
            }
        });
        methods.document("Creates the interval type from postgresql.");
        methods.document("## Params:");
        methods.document("- months: The amount of months in this interval. Defaults to 0");
        methods.document("- days: The amount of days in this interval. Defaults to 0");
        methods
            .document("- microseconds: The amount of microseconds in this interval. Defaults to 0");
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

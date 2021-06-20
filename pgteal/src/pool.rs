use std::sync::{Arc, Mutex};

use async_std::task::block_on;
use futures::prelude::stream::StreamExt;
use mlua::ToLua;
use sqlx::{
    pool::PoolConnection, postgres::PgArguments, query::Query, Executor, PgPool, Postgres,
    Statement,
};
use tealr::{mlu::TealData, TypeName};

use crate::pg_row::LuaRow;

#[derive(Clone, tealr::MluaUserData, TypeName)]
pub(crate) struct Pool {
    pool: PgPool,
}
impl From<PgPool> for Pool {
    fn from(pool: PgPool) -> Self {
        Pool { pool }
    }
}

#[derive(Clone, tealr::MluaUserData, TypeName)]
pub(crate) struct Connection {
    connection: Arc<Mutex<Option<PoolConnection<Postgres>>>>,
}

impl Connection {
    fn drop_con(&self) -> Result<(), mlua::Error> {
        let x = self.connection.lock();
        let mut x = match x {
            Ok(x) => x,
            Err(_) => {
                return Err(mlua::Error::external(crate::base::Error::Custom(
                    "Lock got poisoned".into(),
                )))
            }
        };
        *x = None;
        Ok(())
    }
    fn try_run<X, T: Fn(&mut PoolConnection<Postgres>) -> X>(
        &self,
        func: T,
    ) -> Result<X, mlua::Error> {
        let x = self.connection.lock();
        let mut x = match x {
            Ok(x) => x,
            Err(_) => {
                return Err(mlua::Error::external(crate::base::Error::Custom(
                    "Lock got poisoned".into(),
                )))
            }
        };

        match x.as_mut() {
            Some(x) => Ok(func(x)),
            None => Err(mlua::Error::external(crate::base::Error::Custom(
                "Connection already dropped".into(),
            ))),
        }
    }
    fn run_wrapper<
        T,
        Func: Fn(Query<Postgres, PgArguments>, &mut PoolConnection<Postgres>) -> Result<T, mlua::Error>,
    >(
        &self,
        query: String,
        params: mlua::Value,
        func: Func,
    ) -> Result<T, mlua::Error> {
        self.try_run(move |v| {
            let statement = block_on(v.prepare(&query)).map_err(mlua::Error::external)?;
            let mut query = statement.query();
            if let mlua::Value::Table(x) = &params {
                let needed = statement
                    .parameters()
                    .map(|v| v.map_left(|v| v.len()).left_or_else(|v| v))
                    .unwrap_or(0);
                for k in 1..=needed {
                    let v: mlua::Value = x.get(k)?;
                    query = match v {
                        mlua::Value::Boolean(x) => query.bind(x),
                        mlua::Value::Integer(x) => query.bind(x),
                        mlua::Value::Number(x) => query.bind(x),
                        mlua::Value::String(x) => query.bind(x.to_str()?.to_owned()),
                        mlua::Value::Nil => query.bind::<Option<bool>>(None),
                        x => {
                            return Err(mlua::Error::FromLuaConversionError {
                                from: x.type_name(),
                                to: "bool, number,string",
                                message: Some("Can't store this values in the db".to_string()),
                            })
                        }
                    }
                }
            }
            func(query, v)
        })?
    }
}

impl From<PoolConnection<Postgres>> for Connection {
    fn from(connection: PoolConnection<Postgres>) -> Self {
        Connection {
            connection: Arc::new(Mutex::new(Some(connection))),
        }
    }
}

impl TealData for Connection {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method(
            "fetch_optional",
            |lua, this, (query, params): (String, mlua::Value)| {
                let x = this.run_wrapper(query, params, |query, v| {
                    Ok(block_on(query.fetch_optional(v)).map_err(mlua::Error::external))
                })?;
                match x {
                    Ok(Some(x)) => Ok(Some(LuaRow::from(x).to_lua(lua)?)),
                    Ok(None) => Ok(None),
                    Err(x) => Err(x),
                }
            },
        );
        methods.add_method(
            "fetch_all",
            |lua, this, (query, params): (String, mlua::Value)| {
                this.run_wrapper(query, params, |query, v| {
                    let x: Vec<_> = block_on(
                        query
                            .fetch(v)
                            .map(|v| {
                                v.map_err(mlua::Error::external)
                                    .and_then(|x| crate::pg_row::LuaRow::from(x).to_lua(lua))
                            })
                            .collect(),
                    );
                    Ok(x.into_iter().collect::<Result<Vec<_>, _>>())
                })?
            },
        )
    }
}

impl TealData for Pool {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method("get_connection", |_, me, call_back: mlua::Function| {
            let con = block_on(me.pool.acquire())
                .map_err(crate::base::Error::from)
                .map(Connection::from)?;
            let value = call_back.call::<_, mlua::Value>(con.clone())?;
            con.drop_con()?;

            Ok((true, value))
        })
    }
}

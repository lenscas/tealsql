use std::sync::{Arc, Mutex};

use async_std::task::block_on;
use futures::prelude::stream::StreamExt;
use mlua::ToLua;
use sqlx::{
    pool::PoolConnection, postgres::PgArguments, query::Query, Connection, Executor, Postgres,
    Statement,
};
use tealr::{mlu::TealData, TypeName};

use crate::{internal_connection_wrapper::WrappedConnection, pg_row::LuaRow};

#[derive(Clone)]
pub(crate) struct LuaConnection<'c> {
    connection: Arc<Mutex<Option<WrappedConnection<'c>>>>,
    x: &'c std::marker::PhantomData<()>,
}
impl<'c> TypeName for LuaConnection<'c> {
    //the name of the type as known to teal.
    fn get_type_name(dir: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Connection")
    }
}

impl<'c> mlua::UserData for LuaConnection<'c> {
    //this registers the methods to mlua
    fn add_methods<'c, T: ::mlua::UserDataMethods<'c, Self>>(methods: &mut T) {
        let mut x = tealr::mlu::UserDataWrapper::from_user_data_methods(methods);
        <LuaConnection<'_> as ::tealr::mlu::TealData>::add_methods(&mut x);
    }
}
impl<'c> tealr::TypeBody for LuaConnection<'c> {
    //this allows tealr to generate the type definition for this type
    fn get_type_body(_: ::tealr::Direction, gen: &mut ::tealr::TypeGenerator) {
        gen.is_user_data = true;
        <Self as ::tealr::mlu::TealData>::add_methods(gen);
    }
}

impl<'c> LuaConnection<'c> {
    pub(crate) fn drop_con(&self) -> Result<(), mlua::Error> {
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
    fn try_run<X, T: Fn(&mut WrappedConnection<'c>) -> X>(
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
        Func: Fn(Query<Postgres, PgArguments>, &mut WrappedConnection<'c>) -> Result<T, mlua::Error>,
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

impl<'c> From<PoolConnection<Postgres>> for LuaConnection<'c> {
    fn from(connection: PoolConnection<Postgres>) -> Self {
        LuaConnection {
            connection: Arc::new(Mutex::new(Some(WrappedConnection::PoolConnection(
                connection,
            )))),
            x: &std::marker::PhantomData,
        }
    }
}

impl<'c> TealData for LuaConnection<'c> {
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
        // methods.add_method(
        // "fetch",
        // |lua: &'lua mlua::Lua, this, (query, params): (String, mlua::Value)| {
        // let x = this.connection.lock();
        // let mut x = match x {
        // Ok(x) => x,
        // Err(_) => {
        // return Err(mlua::Error::external(crate::base::Error::Custom(
        // "Lock got poisoned".into(),
        // )))
        // }
        // };
        // let x = match x.as_mut() {
        // Some(x) => Ok(sqlx::query(&query).fetch(x)),
        // None => Err(mlua::Error::external(crate::base::Error::Custom(
        // "Connection already dropped".into(),
        // ))),
        // }?;
        //
        // Ok(crate::iter::Iter::new(x))
        // },
        // );
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
        );
        methods.add_method(
            "execute",
            |_, this, (query, params): (String, mlua::Value)| {
                let x = this.run_wrapper(query, params, |query, v| {
                    Ok(block_on(query.execute(v)).map_err(mlua::Error::external))
                })?;
                match x {
                    Ok(x) => Ok(x.rows_affected()),
                    Err(x) => Err(dbg!(x)),
                }
            },
        );
        methods.add_method("execute_many", |_, this, query: String| {
            let x: Result<Vec<_>, _> = this.try_run(|v| {
                let x = block_on(sqlx::query(&query).execute_many(v));
                block_on(
                    x.map(|v| v.map(|v| v.rows_affected()).map_err(mlua::Error::external))
                        .collect(),
                )
            });
            let x = match x {
                Ok(x) => x,
                Err(x) => return Err(dbg!(x)),
            };
            dbg!(x.into_iter().collect::<Result<Vec<_>, _>>())
        });
        methods.add_method(
            "fetch_one",
            |lua, this, (query, params): (String, mlua::Value)| {
                let x = this.run_wrapper(query, params, |query, v| {
                    Ok(block_on(query.fetch_one(v)).map_err(mlua::Error::external))
                })?;
                match x {
                    Ok(x) => Ok(LuaRow::from(x).to_lua(lua)?),
                    Err(x) => Err(x),
                }
            },
        );
        methods.add_method("begin", |lua, this, func: mlua::Function| {
            this.try_run(|v| {
                let transaction = block_on(v.begin()).map_err(mlua::Error::external)?;
                Ok(())
            })?
        })
    }
}

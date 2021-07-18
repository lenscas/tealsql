use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_std::task::block_on;
use futures::prelude::stream::StreamExt;
use mlua::ToLua;
use parking_lot::{lock_api::MutexGuard, MappedMutexGuard, Mutex};
use sqlx::{
    pool::PoolConnection, postgres::PgArguments, query::Query, Executor, Postgres, Statement,
};
use tealr::{mlu::TealData, TypeName};

use crate::{internal_connection_wrapper::WrappedConnection, pg_row::LuaRow};

fn get_lock<'a>(
    con: &'a Arc<Mutex<Option<WrappedConnection>>>,
) -> Result<MappedMutexGuard<'a, WrappedConnection>, mlua::Error> {
    let x = con.lock();
    parking_lot::lock_api::MutexGuard::<'_, _, _>::try_map(x, |v| v.as_mut()).map_err(|_| {
        mlua::Error::external(crate::base::Error::Custom(
            "Connection already dropped".into(),
        ))
    })
}

fn add_params<'a, 'b, T: Into<ValueHolderOrLuaValue<'b>>>(
    connection: &'a Arc<Mutex<Option<WrappedConnection>>>,
    sql: &'a str,
    params: T,
) -> Result<
    (
        Query<'a, Postgres, PgArguments>,
        MappedMutexGuard<'a, WrappedConnection>,
    ),
    mlua::Error,
> {
    let mut v = get_lock(connection)?;
    let statement = block_on(v.prepare(sql)).map_err(mlua::Error::external)?;
    let mut query = sqlx::query(sql);
    let params = params.into();
    let needed = statement
        .parameters()
        .map(|v| v.map_left(|v| v.len()).left_or_else(|v| v))
        .unwrap_or(0);
    match params {
        ValueHolderOrLuaValue::Lua(params) => {
            if let mlua::Value::Table(x) = &params {
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
        }
        ValueHolderOrLuaValue::Normal(params) => {
            for k in 1..=needed {
                let v = params.get(&(k as i64)).cloned().unwrap_or(ValueHolder::Nil);
                query = match v {
                    ValueHolder::String(x) => query.bind(x),
                    ValueHolder::Number(x) => query.bind(x),
                    ValueHolder::Integer(x) => query.bind(x),
                    ValueHolder::Boolean(x) => query.bind(x),
                    ValueHolder::Nil => query.bind::<Option<bool>>(None),
                }
            }
        }
    }

    Ok((query, v))
}

#[derive(Clone)]
pub(crate) struct LuaConnection<'c> {
    connection: Option<Arc<Mutex<Option<WrappedConnection>>>>,
    x: &'c std::marker::PhantomData<()>,
}
impl<'c> TypeName for LuaConnection<'c> {
    //the name of the type as known to teal.
    fn get_type_name(_: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Connection")
    }
}

impl<'c> mlua::UserData for LuaConnection<'c> {
    //this registers the methods to mlua
    fn add_methods<'lua, T: ::mlua::UserDataMethods<'lua, Self>>(methods: &mut T) {
        let mut x = tealr::mlu::UserDataWrapper::from_user_data_methods(methods);
        <LuaConnection<'_> as ::tealr::mlu::TealData>::add_methods(&mut x);
    }
}
impl tealr::TypeBody for LuaConnection<'static> {
    //this allows tealr to generate the type definition for this type
    fn get_type_body(_: ::tealr::Direction, gen: &mut ::tealr::TypeGenerator) {
        gen.is_user_data = true;
        <Self as ::tealr::mlu::TealData>::add_methods(gen);
    }
}

impl<'c> LuaConnection<'c> {
    pub(crate) fn drop_con(&self) -> Result<(), mlua::Error> {
        let mut x = self
            .connection
            .as_ref()
            .expect("Tried to drop a connection that we do not have access to.")
            .lock();
        *x = None;
        Ok(())
    }
    fn get_lock(&self) -> Result<MappedMutexGuard<WrappedConnection>, mlua::Error> {
        let x = self
            .connection
            .as_ref()
            .ok_or_else(|| {
                mlua::Error::external(crate::base::Error::Custom(
                    "Tried to use a connection that is used for a transaction.".into(),
                ))
            })?
            .lock();
        parking_lot::lock_api::MutexGuard::<'_, _, _>::try_map(x, |v| v.as_mut()).map_err(|_| {
            mlua::Error::external(crate::base::Error::Custom(
                "Connection already dropped".into(),
            ))
        })
    }
    fn add_params<'a>(
        &self,
        sql: &'a str,
        params: mlua::Value,
    ) -> Result<
        (
            Query<'a, Postgres, PgArguments>,
            MappedMutexGuard<WrappedConnection>,
        ),
        mlua::Error,
    > {
        let mut v = self.get_lock()?;
        let statement = block_on(v.prepare(sql)).map_err(mlua::Error::external)?;
        let mut query = sqlx::query(sql);
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
        Ok((query, v))
    }
}

impl<'c> From<PoolConnection<Postgres>> for LuaConnection<'c> {
    fn from(connection: PoolConnection<Postgres>) -> Self {
        LuaConnection {
            connection: Some(Arc::new(Mutex::new(Some(
                WrappedConnection::PoolConnection(connection),
            )))),
            x: &std::marker::PhantomData,
        }
    }
}
impl<'c> From<Arc<Mutex<Option<WrappedConnection>>>> for LuaConnection<'c> {
    fn from(connection: Arc<Mutex<Option<WrappedConnection>>>) -> Self {
        LuaConnection {
            connection: Some(connection),
            x: &std::marker::PhantomData,
        }
    }
}

enum ValueHolderOrLuaValue<'lua> {
    Lua(mlua::Value<'lua>),
    Normal(HashMap<i64, ValueHolder>),
}
impl From<HashMap<i64, ValueHolder>> for ValueHolderOrLuaValue<'static> {
    fn from(x: HashMap<i64, ValueHolder>) -> Self {
        Self::Normal(x)
    }
}

impl<'lua> From<mlua::Value<'lua>> for ValueHolderOrLuaValue<'lua> {
    fn from(x: mlua::Value<'lua>) -> Self {
        Self::Lua(x)
    }
}

#[derive(Clone)]
enum ValueHolder {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Nil,
}

impl ValueHolder {
    fn from_lua_value(value: mlua::Value) -> mlua::Result<Self> {
        Ok(match value {
            mlua::Value::Nil => Self::Nil,
            mlua::Value::Boolean(x) => Self::Boolean(x),
            mlua::Value::Integer(x) => Self::Integer(x),
            mlua::Value::Number(x) => Self::Number(x),
            mlua::Value::String(x) => Self::String(x.to_str()?.to_owned()),
            x => {
                return Err(mlua::Error::FromLuaConversionError {
                    from: x.type_name(),
                    to: "bool, number,string",
                    message: Some("Can't store this values in the db".to_string()),
                })
            }
        })
    }
    fn value_to_map(value: mlua::Value) -> mlua::Result<HashMap<i64, Self>> {
        match value {
            mlua::Value::Table(x) => {
                let mut map = HashMap::new();
                for pair in x.pairs::<mlua::Value, mlua::Value>() {
                    let (k, v) = pair?;
                    if let mlua::Value::Integer(k) = k {
                        let value = Self::from_lua_value(v)?;
                        map.insert(k, value);
                    }
                }
                Ok(map)
            }

            _ => Ok(HashMap::new()),
        }
    }
}

impl<'c> TealData for LuaConnection<'c> {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method(
            "fetch_optional",
            |lua, this, (query, params): (String, mlua::Value)| {
                let (query, mut v) = this.add_params(&query, params)?;
                let x =
                    block_on(query.fetch_optional(v.deref_mut())).map_err(mlua::Error::external);
                match x {
                    Ok(Some(x)) => Ok(Some(LuaRow::from(x).to_lua(lua)?)),
                    Ok(None) => {
                        println!("got Ok(None)");
                        Ok(None)
                    }
                    Err(x) => Err(dbg!(x)),
                }
            },
        );
        methods.add_method(
            "fetch_all",
            |lua, this, (query, params): (String, mlua::Value)| {
                let (query, mut v) = this.add_params(&query, params)?;
                let x: Vec<_> = block_on(
                    query
                        .fetch(v.deref_mut())
                        .map(|v| {
                            v.map_err(mlua::Error::external)
                                .and_then(|x| crate::pg_row::LuaRow::from(x).to_lua(lua))
                        })
                        .collect(),
                );
                x.into_iter().collect::<Result<Vec<_>, _>>()
            },
        );
        methods.add_method(
            "fetch_all_async",
            |lua, this, (query, params): (String, mlua::Value)| {
                let params = ValueHolder::value_to_map(params)?;
                //std::thread::spawn(move || {});
                let connection = this.connection.clone().ok_or_else(|| {
                    mlua::Error::external(crate::base::Error::Custom(
                        "Tried to use a connection that is used for a transaction.".into(),
                    ))
                })?;
                let (sender, rec) = std::sync::mpsc::channel();
                let x = std::thread::spawn(move || {
                    let connection = connection.clone();
                    match add_params(&connection, &query, params) {
                        Ok((query, mut con)) => {
                            let mut stream = query.fetch(con.deref_mut());
                            loop {
                                let next = block_on(stream.next());
                                println!("Got a new row in rust!");
                                let res = match next {
                                    None => break,
                                    Some(Ok(x)) => sender.send(crate::iter::AsyncMessage::Value(x)),
                                    Some(Err(x)) => {
                                        sender.send(crate::iter::AsyncMessage::Error(x))
                                    }
                                };
                                if res.is_err() {
                                    break;
                                }
                            }
                            drop(sender)
                        }
                        Err(x) => {
                            if let mlua::Error::ExternalError(x) = x {
                                let _ = sender.send(crate::iter::AsyncMessage::DynError(x));
                            }
                        }
                    };
                });
                Ok(crate::iter::Iter::new(x, rec))
            },
        );
        methods.add_method(
            "execute",
            |_, this, (query, params): (String, mlua::Value)| {
                let (query, mut v) = this.add_params(&query, params)?;
                let x =
                    dbg!(block_on(query.execute(v.deref_mut())).map_err(mlua::Error::external))?;
                Ok(x.rows_affected())
            },
        );
        methods.add_method(
            "fetch_one",
            |lua, this, (query, params): (String, mlua::Value)| {
                let (query, mut v) = this.add_params(&query, params)?;
                let x = block_on(query.fetch_one(v.deref_mut())).map_err(mlua::Error::external)?;
                LuaRow::from(x).to_lua(lua)
            },
        );
        methods.add_method_mut("begin", |_, this, func: mlua::Function| {
            let connection = this.connection.take().ok_or_else(|| {
                mlua::Error::external(crate::base::Error::Custom(
                    "Tried to use a connection that is used for a transaction.".into(),
                ))
            })?;
            let mut guard = connection.lock();
            let con = match guard.as_mut() {
                Some(con) => con,
                None => {
                    return Err(mlua::Error::external(crate::base::Error::Custom(
                        "Connection already dropped".into(),
                    )))
                }
            };
            let res = block_on(con.execute("BEGIN;"));
            if let Err(x) = res {
                drop(guard);
                this.connection = Some(connection);
                return Err(mlua::Error::external(crate::base::Error::Sqlx(x)));
            }
            drop(guard);
            let lua_con = LuaConnection::from(connection.clone());
            let res: Result<(bool, mlua::Value), _> = func.call(lua_con.clone()).map(|v| match v {
                (mlua::Value::Nil, x) => (true, x),
                (mlua::Value::Boolean(x), y) => (x, y),
                (_, y) => (true, y),
            });
            let mut guard = connection.lock();
            let con = match guard.as_mut() {
                Some(con) => con,
                None => {
                    return Err(mlua::Error::external(crate::base::Error::Custom(
                        "Connection already dropped".into(),
                    )))
                }
            };

            let action = match &res {
                Ok((true, _)) => "COMMIT",
                Ok((false, _)) => "ROLLBACK",
                Err(_) => "ROLLBACK",
            };
            let rollback_res = block_on(con.execute(action));
            drop(guard);
            this.connection = Some(connection);
            match (res, rollback_res) {
                (Err(res_error), Err(rollback_error)) => Err(mlua::Error::external(
                    crate::base::Error::DBErrorAfterHandling(rollback_error, res_error),
                )),
                (Err(res_err), _) => Err(res_err),
                (_, Err(x)) => Err(mlua::Error::external(crate::base::Error::Sqlx(x))),
                (Ok(x), Ok(_)) => Ok(x),
            }
        })
    }
}

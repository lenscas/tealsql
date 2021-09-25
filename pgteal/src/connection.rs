use std::collections::BTreeMap;
use std::{ops::DerefMut, sync::Arc};

use async_std::task::block_on;
use either::Either;
use futures::prelude::stream::StreamExt;
use parking_lot::{MappedMutexGuard, Mutex};
use sqlx::PgConnection;
use sqlx::{
    pool::PoolConnection, postgres::PgArguments, query::Query, Executor, Postgres, Statement,
};
use tealr::mlu::mlua;
use tealr::{
    mlu::{
        mlua::{Integer, Number},
        TealData,
    },
    TypeName,
};

tealr::create_union_mlua!(pub(crate) Derives(PartialEq) enum Input = String | Integer | Number | bool);

pub(crate) type QueryParamCollection = BTreeMap<i64, Input>;

use crate::bind_params::bind_params_on;
use crate::{internal_connection_wrapper::WrappedConnection, iter::Iter, pg_row::LuaRow};

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

fn add_params<'b, 'a: 'b>(
    connection: &'a Arc<Mutex<Option<WrappedConnection>>>,
    sql: &'a str,
    params: &'b mut QueryParamCollection,
) -> Result<
    (
        Query<'b, Postgres, PgArguments>,
        MappedMutexGuard<'a, WrappedConnection>,
    ),
    mlua::Error,
> {
    let mut v = get_lock(connection)?;
    let statement = block_on(v.prepare(sql)).map_err(mlua::Error::external)?;
    let query = sqlx::query(sql);
    let query = bind_params_on(
        params,
        statement.parameters().unwrap_or(Either::Right(0)),
        query,
    )?;
    // let needed = statement
    // .parameters()
    // .map(|v| v.map_left(|v| v.len()).left_or_else(|v| v))
    // .unwrap_or(0);
    //
    // for k in 1..=needed {
    // let v = params.get(&(k as i64));
    // query = match v {
    // Some(Input::String(x)) => query.bind(x),
    // Some(Input::Number(x)) => query.bind(x),
    // Some(Input::Integer(x)) => query.bind(x),
    // Some(Input::bool(x)) => query.bind(x),
    // None => query.bind::<Option<bool>>(None),
    // }
    // }

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
    fn add_methods<'lua, T: mlua::UserDataMethods<'lua, Self>>(methods: &mut T) {
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
            .ok_or_else(|| {
                mlua::Error::external(crate::base::Error::Custom(
                    "Tried to drop a connection that we do not have access to.".to_string(),
                ))
            })?
            .lock();
        *x = None;
        Ok(())
    }
    fn unwrap_connection_option(
        &self,
    ) -> Result<&Arc<Mutex<Option<WrappedConnection>>>, mlua::Error> {
        self.connection.as_ref().ok_or_else(|| {
            mlua::Error::external(crate::base::Error::Custom(
                "Tried to use a connection that is used for a transaction.".into(),
            ))
        })
    }

    fn add_params<'b, 'a: 'b>(
        &'a self,
        sql: &'a str,
        params: &'b mut QueryParamCollection,
    ) -> Result<
        (
            Query<'b, Postgres, PgArguments>,
            MappedMutexGuard<'a, WrappedConnection>,
        ),
        mlua::Error,
    > {
        add_params(self.unwrap_connection_option()?, sql, params)
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

impl<'c> From<sqlx::PgConnection> for LuaConnection<'c> {
    fn from(connection: PgConnection) -> Self {
        LuaConnection {
            connection: Some(Arc::new(Mutex::new(Some(WrappedConnection::Connection(
                connection,
            ))))),
            x: &std::marker::PhantomData,
        }
    }
}

impl<'c> TealData for LuaConnection<'c> {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.add_method(
            "fetch_optional",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = this.add_params(&query, &mut params)?;
                let x =
                    block_on(query.fetch_optional(v.deref_mut())).map_err(mlua::Error::external);
                match x {
                    Ok(Some(x)) => Ok(Some(LuaRow::from(x))),
                    Ok(None) => Ok(None),
                    Err(x) => Err(dbg!(x)),
                }
            },
        );
        methods.add_method(
            "fetch_all",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = this.add_params(&query, &mut params)?;

                let mut stream = query.fetch(v.deref_mut());
                let mut items = Vec::new();
                loop {
                    let next = block_on(stream.next());
                    match next {
                        Some(Ok(x)) => items.push(LuaRow::from(x)),
                        Some(Err(x)) => return Err(mlua::Error::external(x)),
                        None => break,
                    }
                }
                Ok(items)
            },
        );
        methods.add_method(
            "fetch_all_async",
            |_, this, (query, mut params, chunk_count): (String, QueryParamCollection, Option<usize>)| {
                let chunk_count = chunk_count.unwrap_or(1).max(1);
                let connection = this.unwrap_connection_option()?.clone();
                let iter = Iter::from_func(move |sender| {
                    move || {
                        match add_params(&connection, &query, &mut params) {
                            Ok((query, mut con)) => {
                                let mut stream = query
                                    .fetch(con.deref_mut())
                                    .map(|v| match v {
                                        Ok(x) => crate::iter::AsyncMessage::Value(x),
                                        Err(x) => crate::iter::AsyncMessage::Error(x),
                                    })
                                    .chunks(chunk_count);
                                let looper = async {
                                    loop {
                                        match stream.next().await {
                                            None => break,
                                            Some(x) => {
                                                if sender.send(x).is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                };
                                block_on(looper);
                                drop(sender)
                            }
                            Err(x) => {
                                if let mlua::Error::ExternalError(x) = x {
                                    let _ =
                                        sender.send(vec![crate::iter::AsyncMessage::DynError(x)]);
                                }
                            }
                        };
                    }
                });
                Ok(iter)
            },
        );
        methods.add_method(
            "execute",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = this.add_params(&query, &mut params)?;
                let x =
                    dbg!(block_on(query.execute(v.deref_mut())).map_err(mlua::Error::external))?;
                Ok(x.rows_affected())
            },
        );
        methods.add_method(
            "fetch_one",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = this.add_params(&query, &mut params)?;
                let x = block_on(query.fetch_one(v.deref_mut())).map_err(mlua::Error::external)?;
                Ok(LuaRow::from(x))
            },
        );
        methods.add_method_mut(
            "begin",
            |_, this, func: tealr::mlu::TypedFunction<LuaConnection, (Option<bool>, Option<crate::Res>)>| {
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
                let res: Result<(bool, Option<crate::Res>), _> =
                    func.call(lua_con.clone()).map(|v| match v {
                        (None, x) => (true, x),
                        (Some(x), y) => (x, y),
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
            },
        )
    }
}

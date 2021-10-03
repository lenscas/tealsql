use std::collections::BTreeMap;
use std::{ops::DerefMut, sync::Arc};

use async_std::task::block_on;
use either::Either;
use futures::prelude::stream::StreamExt;
use parking_lot::{MappedMutexGuard, Mutex};
use shared::Input;
use sqlx::PgConnection;
use sqlx::{
    pool::PoolConnection, postgres::PgArguments, query::Query, Executor, Postgres, Statement,
};
use tealr::mlu::mlua;
use tealr::{mlu::TealData, TypeName};

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

async fn add_params<'b, 'a: 'b>(
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
    let statement = v.prepare(sql).await.map_err(mlua::Error::external)?;
    let query = sqlx::query(sql);
    let query = bind_params_on(
        params,
        statement.parameters().unwrap_or(Either::Right(0)),
        query,
    )?;

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

    async fn add_params<'b, 'a: 'b>(
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
        add_params(self.unwrap_connection_option()?, sql, params).await
    }
    async fn execute(&self, query: String, mut params: QueryParamCollection) -> mlua::Result<u64> {
        let (query, mut v) = self.add_params(&query, &mut params).await?;
        let x = query
            .execute(v.deref_mut())
            .await
            .map_err(mlua::Error::external)?;
        Ok(x.rows_affected())
    }
    fn extract_lua_to_table_fields(
        values: BTreeMap<String, Input>,
        continue_from: i64,
    ) -> (Vec<String>, Vec<i64>, QueryParamCollection) {
        let x = values.into_iter().collect::<Vec<_>>();
        let keys = x
            .iter()
            .map(|(key, _)| format!("\"{}\"", key))
            .collect::<Vec<_>>();
        let mut markers = Vec::new();
        let values = x
            .into_iter()
            .enumerate()
            .map(|(key, (_, x))| (((key + 1) as i64) + continue_from, x))
            .inspect(|(key, _)| markers.push(*key))
            .collect::<QueryParamCollection>();
        (keys, markers, values)
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
        methods.document("Fetches 1 or 0 results from the database");
        methods.document("Params:");
        methods.document("query: The query string that needs to be executed");
        methods.document(
            "params: An array (table) containing the parameters that this function needs",
        );
        methods.add_method(
            "fetch_optional",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = block_on(this.add_params(&query, &mut params))?;
                let x =
                    block_on(query.fetch_optional(v.deref_mut())).map_err(mlua::Error::external);
                match x {
                    Ok(Some(x)) => Ok(Some(LuaRow::from(x))),
                    Ok(None) => Ok(None),
                    Err(x) => Err(x),
                }
            },
        );
        methods.document("Fetches all results into a table");
        methods.document("Params:");
        methods.document("query: The query string that needs to be executed");
        methods.document(
            "params: An array (table) containing the parameters that this function needs",
        );
        methods.add_method(
            "fetch_all",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                block_on(async move {
                    let (query, mut v) = this.add_params(&query, &mut params).await?;

                    let mut stream = query.fetch(v.deref_mut());
                    let mut items = Vec::new();
                    loop {
                        let next = stream.next().await;
                        match next {
                            Some(Ok(x)) => items.push(LuaRow::from(x)),
                            Some(Err(x)) => return Err(mlua::Error::external(x)),
                            None => break,
                        }
                    }
                    Ok(items)
                })
            },
        );
        methods.document("Runs a thread in the background that fetches all results. Allowing you to consume the results in batches, or do other things while the query is being executed");
        methods.document("Params:");
        methods.document("query: The query string that needs to be executed");
        methods.document(
            "params: An array (table) containing the parameters that this function needs",
        );
        methods.document("chunk_count: How big the batches are that will be returned from the background thread to the main one. Higher batch count may improve performance");
        methods.add_method(
            "fetch_all_async",
            |_, this, (query, mut params, chunk_count): (String, QueryParamCollection, Option<usize>)|  {
                let chunk_count = chunk_count.unwrap_or(1).max(1);
                let connection = this.unwrap_connection_option()?.clone();
                let iter = Iter::from_func(move |sender| {
                    move || {
                        block_on(async {
                            match add_params(&connection, &query, &mut params).await {
                                Ok((query, mut con)) => {
                                    let mut stream = query
                                        .fetch(con.deref_mut())
                                        .map(|v| match v {
                                            Ok(x) => crate::iter::AsyncMessage::Value(x),
                                            Err(x) => crate::iter::AsyncMessage::Error(x),
                                        })
                                        .chunks(chunk_count)
                                        .map(|v|sender.send(v));
                                    
                                    while let Some(item) = stream.next().await {
                                        if item.is_err() {
                                            break
                                        }
                                    }
                                    drop(sender)
                                }
                                Err(x) => {
                                    if let mlua::Error::ExternalError(x) = x {
                                        let _ =
                                            sender.send(vec![crate::iter::AsyncMessage::DynError(x)]);
                                    }
                                }
                            }
                        });
                    }
                });
                Ok(iter)
            },
        );
        methods.document("Fetches exactly 1 value from the database.");
        methods.document("Params:");
        methods.document("query: The query string that needs to be executed");
        methods.document(
            "params: An array (table) containing the parameters that this function needs",
        );
        methods.add_method(
            "execute",
            |_, this, (query, params): (String, QueryParamCollection)| {
                block_on(this.execute(query, params))
            },
        );
        methods.document("Params:");
        methods.document("query: The query string that needs to be executed");
        methods.document(
            "params: An array (table) containing the parameters that this function needs",
        );
        methods.add_method(
            "fetch_one",
            |_, this, (query, mut params): (String, QueryParamCollection)| {
                let (query, mut v) = block_on(this.add_params(&query, &mut params))?;
                let x = block_on(query.fetch_one(v.deref_mut())).map_err(mlua::Error::external)?;
                Ok(LuaRow::from(x))
            },
        );
        methods.document("Starts a new transaction.");
        methods.document("Params:");
        methods.document(
            "func: The function that will be executed after the transaction has been made.",
        );
        methods.document("This function can return 2 values, the first is a boolean that determines if the transaction should be committed or not.");
        methods.document("The second can be of any type and will be returned as is");
        methods.document("After this function is executed the transaction will either be committed or rolled back.");
        methods.document("It will be rolled back if the callback threw an error, or returned false for the first return value");
        methods.document("Otherwise, it will be committed");
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
        );
        methods.document("A shorthand to run a basic insert command.");
        methods.document("WARNING!:");
        methods.document("the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.");
        methods.document("The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.");
        methods.document("Parameters:");
        methods.document("name: the table name that will be inserted into");
        methods.document("values: A table where the keys are the column names and the values are the values that will be inserted");
        methods.add_method(
            "insert",
            |_, this, (name, values): (String, BTreeMap<String, Input>)| {
                let (keys, markers, values) = Self::extract_lua_to_table_fields(values, 0);
                let sql = format!(
                    "INSERT INTO \"{}\" ({}) VALUES ({})",
                    name,
                    keys.join(","),
                    markers
                        .into_iter()
                        .map(|v| format!("${}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                block_on(this.execute(sql, values))
            },
        );
        methods.document("A shorthand to run a basic update command.");
        methods.document("WARNING!:");
        methods.document("the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.");
        methods.document("The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.");
        methods.document("Parameters:");
        methods.document("name: the table name that will be inserted into");
        methods.document("old_values: A table used to construct the `where` part of the query. The keys are the column names and the values are the values that will be matched against");
        methods.document("new_values: A table where the keys are the column names and the values are the values that this column will be updated to");
        methods.add_method(
            "update",
            |_,
             this,
             (name, old_values, new_values): (
                String,
                BTreeMap<String, Input>,
                BTreeMap<String, Input>,
            )| {
                let (old_keys, old_markers, mut old_values) =
                    Self::extract_lua_to_table_fields(old_values, 0);
                let (new_keys, new_markers, mut new_values) =
                    Self::extract_lua_to_table_fields(new_values, (old_markers.len()) as i64);
                let sql = format!(
                    "UPDATE \"{}\"
                    SET {}
                WHERE {};
                ",
                    name,
                    new_keys
                        .into_iter()
                        .zip(new_markers.iter())
                        .map(|(key, marker)| format!("{} = ${}", key, marker))
                        .collect::<Vec<_>>()
                        .join(","),
                    old_keys
                        .into_iter()
                        .zip(old_markers.iter())
                        .map(|(key, marker)| format!("{} = ${}", key, marker))
                        .collect::<Vec<_>>()
                        .join("\n AND ")
                );
                new_values.append(&mut old_values);
                block_on(this.execute(sql, new_values))
            },
        );
        methods.document("A shorthand to run a basic delete command.");
        methods.document("WARNING!:");
        methods.document("the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.");
        methods.document("The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.");
        methods.document("Parameters:");
        methods.document("name: the table name that will be inserted into");
        methods.document("old_values: A table used to construct the `where` part of the query. The keys are the column names and the values are the values that will be matched against");
        methods.add_method(
            "delete",
            |_, this, (name, check_on): (String, BTreeMap<String, Input>)| {
                let (keys, markers, values) = Self::extract_lua_to_table_fields(check_on, 0);
                let where_parts = keys
                    .into_iter()
                    .zip(markers.into_iter())
                    .map(|(key, marker)| format!("{} = ${}", key, marker))
                    .collect::<Vec<_>>()
                    .join("\n AND ");
                let sql = format!(
                    "DELETE FROM \"{}\"
                WHERE {};
                ",
                    name, where_parts
                );
                block_on(this.execute(sql, values))
            },
        );
        methods.generate_help();
    }
}

use std::sync::Arc;

use sqlx::PgPool;
use tealr::{mlu::TealData, TypeName};
use tokio::runtime::Runtime;

use crate::connection::LuaConnection;

#[derive(Clone, tealr::mlu::UserData, TypeName)]
pub(crate) struct Pool {
    pool: PgPool,
    runtime: Arc<Runtime>,
}
// impl From<PgPool> for Pool {
// fn from(pool: PgPool) -> Self {
// Pool { pool }
// }
// }

impl Pool {
    pub(crate) fn new(pool: PgPool, runtime: Arc<Runtime>) -> Self {
        Pool { pool, runtime }
    }
}

impl TealData for Pool {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
        methods.document_type("A connection pool containing at most 10 connections.");
        methods.document_type("Opening a database connection for each and every operation to the database can quickly become expensive.");
        methods.document_type("A connection pool is a standard technique that can manage opening and re-using connections. Normally it also enforces a maximum number of connections as these are an expensive resource on the database server.");

        methods.document("Gets a connection from the pool");
        methods.document("Parameters:");
        methods.document(
            "call_back: The function that will be executed after the connection has been made.",
        );
        methods.document("This function receives the connection object, which will be cleaned up after the function has been executed.");
        methods.document(
            "A value returned from this function will also be returned by the connect function",
        );
        methods.add_method(
            "get_connection",
            |_, me, call_back: tealr::mlu::TypedFunction<LuaConnection, crate::Res>| {
                let con = me
                    .runtime
                    .block_on(me.pool.acquire())
                    .map_err(crate::base::Error::from)
                    .map(|v| LuaConnection::from_pool(v, me.runtime.clone()))?;
                let value = call_back.call(con.clone())?;
                con.drop_con()?;

                Ok(value)
            },
        );
        methods.generate_help();
    }
}

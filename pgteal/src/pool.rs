use async_std::task::block_on;

use sqlx::PgPool;
use tealr::{mlu::mlua, mlu::TealData, TypeName};

use crate::connection::LuaConnection;

#[derive(Clone, tealr::MluaUserData, TypeName)]
pub(crate) struct Pool {
    pool: PgPool,
}
impl From<PgPool> for Pool {
    fn from(pool: PgPool) -> Self {
        Pool { pool }
    }
}

impl TealData for Pool {
    fn add_methods<'lua, T: tealr::mlu::TealDataMethods<'lua, Self>>(methods: &mut T) {
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
                let con = block_on(me.pool.acquire())
                    .map_err(crate::base::Error::from)
                    .map(LuaConnection::from)?;
                let value = call_back.call(con.clone())?;
                con.drop_con()?;

                Ok(value)
            },
        );
        methods.generate_help();
    }
}

mod base;
mod connection;
mod internal_connection_wrapper;
mod iter;
mod pg_row;
mod pool;
mod value_holder;

use std::string::FromUtf8Error;

use mlua::{Lua, Result as LuaResult};
use tealr::{Direction, TypeWalker};

#[mlua::lua_module]
fn libpgteal(_: &Lua) -> LuaResult<base::Base> {
    let x = base::Base {};
    Ok(x)
}

pub fn generate_types() -> Result<String, FromUtf8Error> {
    let types = TypeWalker::new()
        .process_type::<base::Base>(Direction::ToLua)
        .process_type::<crate::pool::Pool>(Direction::ToLua)
        .process_type::<crate::connection::LuaConnection>(Direction::ToLua);
    types.generate_local("libpgteal")
}

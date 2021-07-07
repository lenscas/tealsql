mod base;
mod connection;
//mod connection2;
mod internal_connection_wrapper;
mod iter;
mod pg_row;
mod pool;

use mlua::{Lua, Result as LuaResult};

#[mlua::lua_module]
fn libpgteal(_: &Lua) -> LuaResult<base::Base> {
    let x = base::Base {};
    Ok(x)
}

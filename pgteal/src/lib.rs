mod base;
mod bind_params;
mod connection;
mod internal_connection_wrapper;
mod iter;
mod pg_row;
mod pool;

pub use base::Base;

use std::string::FromUtf8Error;

use tealr::{
    mlu::mlua::{Lua, Result as LuaResult},
    Direction, TypeWalker,
};

#[tealr::mlu::mlua::lua_module]
fn libpgteal(_: &Lua) -> LuaResult<Base> {
    let x = Base {};
    Ok(x)
}

pub fn generate_types() -> Result<String, FromUtf8Error> {
    let types = TypeWalker::new()
        .process_type_inline::<base::Base>(Direction::ToLua)
        .process_type::<crate::pool::Pool>(Direction::ToLua)
        .process_type::<crate::connection::LuaConnection>(Direction::ToLua)
        .process_type::<crate::iter::Iter>(Direction::ToLua);
    types.generate_local("libpgteal")
}
tealr::create_generic_mlua!(pub(crate) Res);

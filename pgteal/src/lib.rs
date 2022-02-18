mod base;
mod bind_params;
mod connection;
mod internal_connection_wrapper;
mod iter;
mod pg_row;
mod pool;

use std::string::FromUtf8Error;

pub use base::Base;

use tealr::{
    mlu::mlua::{Lua, Result as LuaResult},
    Direction, TypeWalker,
};

#[tealr::mlu::mlua::lua_module]
fn libpgteal(_: &Lua) -> LuaResult<Base> {
    let x = Base {};
    Ok(x)
}

pub(crate) fn generate_types() -> TypeWalker {
    TypeWalker::new()
        .process_type_inline::<base::Base>(Direction::ToLua)
        .process_type::<crate::pool::Pool>(Direction::ToLua)
        .process_type::<crate::connection::LuaConnection>(Direction::ToLua)
        .process_type::<crate::iter::Iter>(Direction::ToLua)
        .process_type::<shared::Interval>(Direction::ToLua)
}

pub fn generate_defs() -> Result<String, FromUtf8Error> {
    let types = generate_types();
    types.generate_local("libpgteal")
}
pub fn generate_json(pretty: bool) -> Result<String, serde_json::Error> {
    let types = generate_types();
    if pretty {
        serde_json::to_string_pretty(&types)
    } else {
        serde_json::to_string(&pretty)
    }
}

tealr::create_generic_mlua!(pub(crate) Res);

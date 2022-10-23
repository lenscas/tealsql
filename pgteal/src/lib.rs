mod base;
mod bind_params;
mod connection;
mod internal_connection_wrapper;
mod iter;
mod pg_row;
mod pool;

pub use base::Base;

use tealr::{
    mlu::mlua::{Lua, Result as LuaResult},
    TypeWalker,
};

#[tealr::mlu::mlua::lua_module]
fn libpgteal(_: &Lua) -> LuaResult<Base> {
    let x = Base {};
    Ok(x)
}

pub(crate) fn generate_types() -> TypeWalker {
    TypeWalker::new()
        .process_type_inline::<base::Base>()
        .process_type::<crate::pool::Pool>()
        .process_type::<crate::connection::LuaConnection>()
        .process_type::<crate::iter::Iter>()
        .process_type::<shared::Interval>()
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

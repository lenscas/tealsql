use std::borrow::Cow;

use sqlx_core::postgres::types::PgInterval;
use tealr::{
    mlu::mlua::{FromLua, ToLua, Value},
    TypeName,
};

pub struct Interval(pub PgInterval);

impl From<PgInterval> for Interval {
    fn from(x: PgInterval) -> Self {
        Self(x)
    }
}
impl From<Interval> for PgInterval {
    fn from(x: Interval) -> Self {
        x.0
    }
}

impl TypeName for Interval {
    fn get_type_name(_: tealr::Direction) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("Interval")
    }
}
impl<'lua> FromLua<'lua> for Interval {
    fn from_lua(
        lua_value: tealr::mlu::mlua::Value<'lua>,
        _: &'lua tealr::mlu::mlua::Lua,
    ) -> tealr::mlu::mlua::Result<Self> {
        if let Value::Table(x) = lua_value {
            Ok(PgInterval {
                months: x.get::<_, i32>("months")?,
                days: x.get::<_, i32>("days")?,
                microseconds: x.get::<_, i64>("microseconds")?,
            }
            .into())
        } else {
            Err(tealr::mlu::mlua::Error::FromLuaConversionError {
                from: lua_value.type_name(),
                to: "Interval",
                message: None,
            })
        }
    }
}
impl<'lua> ToLua<'lua> for Interval {
    fn to_lua(self, lua: &'lua tealr::mlu::mlua::Lua) -> tealr::mlu::mlua::Result<Value<'lua>> {
        let table = lua.create_table()?;
        table.set("months", self.0.months)?;
        table.set("days", self.0.days)?;
        table.set("microseconds", self.0.microseconds)?;
        table.to_lua(lua)
    }
}

impl<'lua> tealr::TypeBody for Interval {
    fn get_type_body(_: tealr::Direction, gen: &mut tealr::TypeGenerator) {
        gen.fields
            .push((Cow::Borrowed("months"), Cow::Borrowed("integer")));
        gen.fields
            .push((Cow::Borrowed("days"), Cow::Borrowed("integer")));
        gen.fields
            .push((Cow::Borrowed("microseconds"), Cow::Borrowed("integer")));
    }
}

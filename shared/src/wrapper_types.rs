use std::{borrow::Cow, convert::TryFrom};

use sqlx_core::postgres::types::PgInterval;
use tealr::{
    mlu::mlua::{FromLua, ToLua, Value},
    TypeName,
};

use crate::Table;

#[derive(Clone, PartialEq, Eq, Debug)]
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

fn get_interval_part(value: &Table, index: &str) -> tealr::mlu::mlua::Result<i64> {
    value.0.get(index).map(|v| {
        v.as_i64().ok_or_else(|| {
            tealr::mlu::mlua::Error::FromLuaConversionError {
                from: "unknown",
                to: "integer",
                message: Some(
                    format!(
                        "Tried to convert {} to integer while constructing an `Interval` for field `{}`",
                        serde_json::to_string_pretty(v)
                            .unwrap_or_else(
                                |_|"unknown".to_string()
                            ),
                        index
                        )
                ),
            }
        })
    }).unwrap_or(Ok(0))
}

impl TryFrom<Table> for Interval {
    type Error = tealr::mlu::mlua::Error;

    fn try_from(value: Table) -> Result<Self, Self::Error> {
        Ok(PgInterval {
            months: get_interval_part(&value, "months")? as i32,
            days: get_interval_part(&value, "days")? as i32,
            microseconds: get_interval_part(&value, "microseconds")?,
        }
        .into())
    }
}

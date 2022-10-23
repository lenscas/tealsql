use std::{borrow::Cow, convert::TryFrom};

use serde::Serialize;
use sqlx_core::postgres::types::PgInterval;
use tealr::{
    mlu::mlua::{FromLua, ToLua, Value},
    new_type, Field, NamePart, RecordGenerator, TypeName,
};

#[derive(Serialize)]
#[serde(remote = "PgInterval")]
struct IntervalDefForSerde {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

use crate::Table;

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct Interval(#[serde(with = "IntervalDefForSerde")] pub PgInterval);

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
    fn get_type_parts() -> std::borrow::Cow<'static, [NamePart]> {
        new_type!(Interval)
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

impl tealr::TypeBody for Interval {
    fn get_type_body() -> tealr::TypeGenerator {
        let mut a = RecordGenerator::new::<Self>(false);
        a.fields.push(Field {
            name: Cow::Borrowed("months").into(),
            teal_type: i32::get_type_parts(),
        });
        a.fields.push(Field {
            name: Cow::Borrowed("days").into(),
            teal_type: i32::get_type_parts(),
        });
        a.fields.push(Field {
            name: Cow::Borrowed("microseconds").into(),
            teal_type: i64::get_type_parts(),
        });
        tealr::TypeGenerator::Record(Box::new(a))
    }

    // fn get_type_body(gen: &mut tealr::TypeGenerator) {
    //     gen.fields
    //         .push((Cow::Borrowed("months"), Cow::Borrowed("integer")));
    //     gen.fields
    //         .push((Cow::Borrowed("days"), Cow::Borrowed("integer")));
    //     gen.fields
    //         .push((Cow::Borrowed("microseconds"), Cow::Borrowed("integer")));
    // }
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

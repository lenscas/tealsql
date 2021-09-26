mod wrapper_types;
use std::convert::TryInto;

use serde::de::DeserializeOwned;
use sqlx_core::{
    encode::Encode,
    postgres::{
        types::{PgInterval, PgMoney},
        PgArguments, PgTypeInfo, PgValue, Postgres,
    },
    query::Query,
    type_info::TypeInfo,
    types::Type,
    value::Value,
};
use tealr::mlu::mlua::{self, Integer, LuaSerdeExt, Number, ToLua};
use uuid::Uuid;

pub use wrapper_types::Interval;

#[derive(PartialEq, Clone, Debug)]
pub struct Table(pub serde_json::Value);
impl<'lua> mlua::FromLua<'lua> for Table {
    fn from_lua(
        val: mlua::Value<'lua>,
        lua: &'lua mlua::Lua,
    ) -> std::result::Result<Self, mlua::Error> {
        let v = LuaSerdeExt::from_value::<serde_json::Value>(lua, val).map_err(|v| dbg!(v))?;
        Ok(Self(v))
    }
}
impl<'lua> mlua::ToLua<'lua> for Table {
    fn to_lua(self, lua: &'lua mlua::Lua) -> mlua::Result<mlua::Value<'lua>> {
        LuaSerdeExt::to_value(lua, &self.0)
    }
}
impl tealr::TypeName for Table {
    fn get_type_name(_: tealr::Direction) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("{any:any}")
    }
}

tealr::create_union_mlua!(pub Derives(PartialEq,Debug) enum Input = String | Table  | Integer | Number | bool);

pub enum TypeInformation {
    BOOL,
    CHARINT,
    SMALLINT,
    INT,
    BIGINT,
    REAL,
    DOUBLE,
    VARCHAR,
    BYTEA,
    MONEY,
    UUID,
    JSON,
    INTERVAL,
    Unknown,
    BOOLArray,
    CHARINTArray,
    SMALLINTArray,
    INTArray,
    BIGINTArray,
    REALArray,
    DOUBLEArray,
    VARCHARArray,
    BYTEAArray,
    MONEYArray,
    UUIDArray,
    JSONArray,
    INTERVALArray,
}

fn c<'lua, X: ToLua<'lua>>(lua: &'lua mlua::Lua) -> impl Fn(X) -> mlua::Result<mlua::Value<'lua>> {
    move |x| x.to_lua(lua)
}

fn try_bind<'q, T, X>(
    query: Query<'q, Postgres, PgArguments>,
    value: X,
) -> Result<Query<'q, Postgres, PgArguments>, mlua::Error>
where
    T: 'q + Send + Encode<'q, Postgres> + Type<Postgres> + tealr::TypeName,
    X: TryInto<T>,
{
    match value.try_into() {
        Ok(x) => Ok(query.bind(x)),
        Err(_) => {
            let from = match T::get_type_name(tealr::Direction::FromLua) {
                std::borrow::Cow::Borrowed(x) => x,
                std::borrow::Cow::Owned(_) => "unknown",
            };
            Err(mlua::Error::FromLuaConversionError {
                from,
                to: "unknown",
                message: None,
            })
        }
    }
}

fn try_json_to_array_of<T: DeserializeOwned>(json: serde_json::Value) -> Result<T, mlua::Error> {
    serde_json::from_value(json).map_err(mlua::Error::external)
}
fn bind_array_of<
    'a,
    T: 'static + DeserializeOwned + Send + Encode<'a, Postgres> + Type<Postgres>,
>(
    query: Query<'a, Postgres, PgArguments>,
    data: Table,
) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
    let as_vec = try_json_to_array_of::<T>(data.0)?;
    Ok(query.bind(as_vec))
}

impl TypeInformation {
    pub fn parse_maybe_str(v: Option<&str>) -> Option<TypeInformation> {
        match v {
            Some(x) => Self::parse_str(x),
            None => Some(TypeInformation::Unknown),
        }
    }
    pub fn parse_str(v: &str) -> Option<TypeInformation> {
        let v = match v {
            "BOOL" => Self::BOOL,
            "\"CHAR\"" => Self::CHARINT,
            "SMALLINT" | "SMALLSERIAL" | "INT2" => Self::SMALLINT,
            "INT" | "SERIAL" | "INT4" => Self::INT,
            "BIGINT" | "BIGSERIAL" | "INT8" => Self::BIGINT,
            "REAL" | "FLOAT4" => Self::REAL,
            "DOUBLE PRECISION" | "FLOATS" => Self::DOUBLE,
            "VARCHAR" | "CHAR" | "TEXT" | "NAME" => Self::VARCHAR,
            "BYTEA" => Self::BYTEA,
            "MONEY" => Self::MONEY,
            "UUID" => Self::UUID,
            "JSON" | "JSONB" => Self::JSON,
            "INTERVAL" => Self::INTERVAL,
            "BOOL[]" => Self::BOOLArray,
            "\"CHAR\"[]" => Self::CHARINTArray,
            "SMALLINT[]" | "SMALLSERIAL[]" | "INT2[]" => Self::SMALLINTArray,
            "INT[]" | "SERIAL[]" | "INT4[]" => Self::INTArray,
            "BIGINT[]" | "BIGSERIAL[]" | "INT8[]" => Self::BIGINTArray,
            "REAL[]" | "FLOAT4[]" => Self::REALArray,
            "DOUBLE PRECISION[]" | "FLOATS[]" => Self::DOUBLEArray,
            "VARCHAR[]" | "CHAR[]" | "TEXT[]" | "NAME[]" => Self::VARCHARArray,
            "BYTEA[]" => Self::BYTEAArray,
            "MONEY[]" => Self::MONEYArray,
            "UUID[]" => Self::UUIDArray,
            "JSON[]" | "JSONB[]" => Self::JSONArray,
            "INTERVAL[]" => Self::INTERVALArray,
            _ => return None,
        };
        Some(v)
    }
    pub fn as_lua(&self) -> String {
        match self {
            TypeInformation::BOOL => "bool".to_string(),
            TypeInformation::CHARINT => "integer".to_string(),
            TypeInformation::SMALLINT => "integer".to_string(),
            TypeInformation::INT => "integer".to_string(),
            TypeInformation::BIGINT => "integer".to_string(),
            TypeInformation::REAL => "number".to_string(),
            TypeInformation::DOUBLE => "number".to_string(),
            TypeInformation::VARCHAR => "string".to_string(),
            TypeInformation::BYTEA => "{integer}".to_string(),
            TypeInformation::MONEY => "integer".to_string(),
            TypeInformation::UUID => "string".to_string(),
            TypeInformation::JSON => "any".to_string(),
            TypeInformation::INTERVAL => "libpgteal.Interval".to_string(),
            TypeInformation::Unknown => "any".to_string(),
            TypeInformation::BOOLArray => format!("{{{}}}", Self::BOOL.as_lua()),
            TypeInformation::CHARINTArray => format!("{{{}}}", Self::CHARINT.as_lua()),
            TypeInformation::SMALLINTArray => format!("{{{}}}", Self::SMALLINT.as_lua()),
            TypeInformation::INTArray => format!("{{{}}}", Self::INT.as_lua()),
            TypeInformation::BIGINTArray => format!("{{{}}}", Self::BIGINT.as_lua()),
            TypeInformation::REALArray => format!("{{{}}}", Self::REAL.as_lua()),
            TypeInformation::DOUBLEArray => format!("{{{}}}", Self::DOUBLE.as_lua()),
            TypeInformation::VARCHARArray => format!("{{{}}}", Self::VARCHAR.as_lua()),
            TypeInformation::BYTEAArray => format!("{{{}}}", Self::BYTEA.as_lua()),
            TypeInformation::MONEYArray => format!("{{{}}}", Self::MONEY.as_lua()),
            TypeInformation::UUIDArray => format!("{{{}}}", Self::UUID.as_lua()),
            TypeInformation::JSONArray => format!("{{{}}}", Self::JSON.as_lua()),
            TypeInformation::INTERVALArray => format!("{{{}}}", Self::INTERVAL.as_lua()),
        }
    }
    pub fn decode(
        value: PgValue,
        l: &tealr::mlu::mlua::Lua,
    ) -> tealr::mlu::mlua::Result<tealr::mlu::mlua::Value<'_>> {
        let v = value.type_info();
        let name = v.name();
        let name = Self::parse_str(name).ok_or(tealr::mlu::mlua::Error::ToLuaConversionError {
            from: "unknown",
            to: "unknown",
            message: Some(format!(
                "Got an unknown type back from postgresql. Typename:{}",
                v.name()
            )),
        })?;
        match name {
            TypeInformation::BOOL => value.try_decode::<bool>().map(c(l)),
            TypeInformation::CHARINT => value.try_decode::<i8>().map(c(l)),
            TypeInformation::SMALLINT => value.try_decode::<i16>().map(c(l)),
            TypeInformation::INT => value.try_decode::<i32>().map(c(l)),
            TypeInformation::BIGINT => value.try_decode::<i64>().map(c(l)),
            TypeInformation::REAL => value.try_decode::<f32>().map(c(l)),
            TypeInformation::DOUBLE => value.try_decode::<f64>().map(c(l)),
            TypeInformation::VARCHAR => value.try_decode::<String>().map(c(l)),
            TypeInformation::BYTEA => value.try_decode::<Vec<u8>>().map(c(l)),
            TypeInformation::MONEY => value.try_decode::<PgMoney>().map(|v| v.0).map(c(l)),
            TypeInformation::INTERVAL => value
                .try_decode::<PgInterval>()
                .map(Interval::from)
                .map(c(l)),
            TypeInformation::UUID => value
                .try_decode::<uuid::Uuid>()
                .map(|v| v.to_string())
                .map(c(l)),
            TypeInformation::JSON => value
                .try_decode::<serde_json::Value>()
                .map(|v| dbg!(v))
                .map(|v| l.to_value_with(&v, Default::default())),

            TypeInformation::BOOLArray => value.try_decode::<Vec<bool>>().map(c(l)),
            TypeInformation::CHARINTArray => value.try_decode::<Vec<i8>>().map(c(l)),
            TypeInformation::SMALLINTArray => value.try_decode::<Vec<i16>>().map(c(l)),
            TypeInformation::INTArray => value.try_decode::<Vec<i32>>().map(c(l)),
            TypeInformation::BIGINTArray => value.try_decode::<Vec<i64>>().map(c(l)),
            TypeInformation::REALArray => value.try_decode::<Vec<f32>>().map(c(l)),
            TypeInformation::DOUBLEArray => value.try_decode::<Vec<f64>>().map(c(l)),
            TypeInformation::VARCHARArray => value.try_decode::<Vec<String>>().map(c(l)),
            TypeInformation::BYTEAArray => value.try_decode::<Vec<Vec<u8>>>().map(c(l)),

            TypeInformation::INTERVALArray => {
                return Err(tealr::mlu::mlua::Error::ToLuaConversionError {
                    from: "INTERVAL[]",
                    to: "{Interval}",
                    message: Some(String::from("At the moment INTERVAL[]'s can't be decoded")),
                })
            }
            TypeInformation::MONEYArray => value
                .try_decode::<Vec<PgMoney>>()
                .map(|v| v.into_iter().map(|v| v.0).collect::<Vec<_>>())
                .map(c(l)),
            TypeInformation::UUIDArray => value
                .try_decode::<Vec<uuid::Uuid>>()
                .map(|v| v.iter().map(ToString::to_string).collect::<Vec<_>>())
                .map(c(l)),
            TypeInformation::JSONArray => value
                .try_decode::<Vec<serde_json::Value>>()
                .map(|v| dbg!(v))
                .map(|v| {
                    v.iter()
                        .map(|v| l.to_value(v))
                        .collect::<Result<Vec<_>, _>>()
                })
                .map(|v| v.and_then(|v| v.to_lua(l))),

            TypeInformation::Unknown => unreachable!(),
        }
        .map_err(mlua::Error::external)?
    }
    pub fn bind_on<'a>(
        param_type: Option<Input>,
        info: Option<&PgTypeInfo>,
        mut query: Query<'a, Postgres, PgArguments>,
    ) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
        let info = TypeInformation::parse_maybe_str(info.map(TypeInfo::name)).ok_or(
            tealr::mlu::mlua::Error::FromLuaConversionError {
                from: "unknown",
                to: "unknown",
                message: Some(format!(
                    "Don't know how to convert to {}",
                    info.map(|v| v.name()).unwrap_or("unknown")
                )),
            },
        )?;
        query = match (param_type, info) {
            (Some(Input::bool(x)), TypeInformation::BOOL | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Integer(x)), TypeInformation::CHARINT) => try_bind::<i8, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::SMALLINT) => try_bind::<i16, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::INT) => try_bind::<i32, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::BIGINT | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Number(x)), TypeInformation::REAL) => query.bind(x as f32),
            (Some(Input::Number(x)), TypeInformation::DOUBLE | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::String(x)), TypeInformation::VARCHAR | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Table(x)), TypeInformation::JSON | TypeInformation::Unknown) => {
                query.bind(x.0)
            }
            (Some(Input::Integer(x)), TypeInformation::MONEY) => query.bind(PgMoney(x)),
            (None, _) => query.bind::<Option<bool>>(None),
            (Some(Input::String(x)), TypeInformation::UUID) => Uuid::parse_str(&x)
                .map_err(mlua::Error::external)
                .map(|v| query.bind(v))?,
            (Some(Input::Table(data)), info) => match info {
                TypeInformation::BOOLArray => bind_array_of::<bool>(query, data)?,
                TypeInformation::CHARINTArray => bind_array_of::<i8>(query, data)?,
                TypeInformation::SMALLINTArray => bind_array_of::<i16>(query, data)?,
                TypeInformation::INTArray => bind_array_of::<i32>(query, data)?,
                TypeInformation::BIGINTArray => bind_array_of::<i64>(query, data)?,
                TypeInformation::REALArray => bind_array_of::<f32>(query, data)?,
                TypeInformation::DOUBLEArray => bind_array_of::<f64>(query, data)?,
                TypeInformation::VARCHARArray => bind_array_of::<String>(query, data)?,
                TypeInformation::JSONArray => bind_array_of::<serde_json::Value>(query, data)?,
                TypeInformation::MONEYArray => {
                    let res = serde_json::from_value::<Vec<i64>>(data.0)
                        .map_err(mlua::Error::external)?
                        .into_iter()
                        .map(PgMoney)
                        .collect::<Vec<_>>();
                    query.bind(res)
                }
                _ => {
                    return Err(mlua::Error::FromLuaConversionError {
                        from: "unknown",
                        to: "unknown",
                        message: None,
                    })
                }
            },
            (_, _) => {
                return Err(mlua::Error::FromLuaConversionError {
                    from: "unknown",
                    to: "unknown",
                    message: None,
                })
            }
        };
        Ok(query)
    }
}

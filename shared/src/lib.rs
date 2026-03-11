mod wrapper_types;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use sqlx::{
    encode::Encode,
    postgres::{
        types::{PgInterval, PgMoney},
        PgArguments, PgTypeInfo, PgValue, Postgres,
    },
    query::Query,
    types::Type,
    TypeInfo, Value,
};
use tealr::mlu::mlua::{self, FromLua, IntoLua, LuaSerdeExt};
use tealr::mlu::FromLuaExact;
use tealr::ToTypename;
use uuid::Uuid;

pub use wrapper_types::Interval;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Table(pub serde_json::Value);
impl mlua::FromLua for Table {
    fn from_lua(val: mlua::Value, lua: &mlua::Lua) -> std::result::Result<Self, mlua::Error> {
        if let mlua::Value::Nil
        | mlua::Value::Boolean(_)
        | mlua::Value::LightUserData(_)
        | mlua::Value::Integer(_)
        | mlua::Value::Number(_)
        | mlua::Value::String(_)
        | mlua::Value::Function(_)
        | mlua::Value::Thread(_)
        | mlua::Value::UserData(_)
        | mlua::Value::Error(_) = val
        {
            return Err(mlua::Error::FromLuaConversionError {
                from: val.type_name(),
                to: "table".into(),
                message: None,
            });
        }
        let v = LuaSerdeExt::from_value::<serde_json::Value>(lua, val)?;
        Ok(Self(v))
    }
}
impl mlua::IntoLua for Table {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        LuaSerdeExt::to_value(lua, &self.0)
    }
}

impl tealr::mlu::FromLuaExact for Table {
    fn from_lua_exact(
        value: tealr::mlu::mlua::Value,
        lua: &mlua::Lua,
    ) -> std::result::Result<Self, mlua::Error> {
        Self::from_lua(value, lua)
    }
}

impl tealr::ToTypename for Table {
    fn to_typename() -> tealr::Type {
        tealr::mlu::mlua::Table::to_typename()
    }
}
#[derive(Debug)]
pub enum Input {
    Table(Table),
    Boolean(bool),
    Integer(i64),
    Number(f64),
    String(String),
}

impl ToTypename for Input {
    fn to_typename() -> tealr::Type {
        tealr::Type::new_single("any", tealr::KindOfType::Builtin)
    }
}
impl FromLua for Input {
    fn from_lua(value: mlua::Value, lua: &mlua::Lua) -> std::result::Result<Self, mlua::Error> {
        Ok(match value {
            mlua::Value::Table(t) => Input::Table(Table::from_lua(mlua::Value::Table(t), lua)?),
            mlua::Value::Boolean(b) => Input::Boolean(b),
            mlua::Value::Integer(i) => Input::Integer(i),
            mlua::Value::Number(n) => Input::Number(n),
            mlua::Value::String(s) => Input::String(String::from_lua(mlua::Value::String(s), lua)?),
            _ => {
                return Err(mlua::Error::FromLuaConversionError {
                    from: value.type_name(),
                    to: "any postgresql compatible data".into(),
                    message: None,
                })
            }
        })
    }
}
impl FromLuaExact for Input {
    fn from_lua_exact(
        value: mlua::Value,
        lua: &mlua::Lua,
    ) -> std::result::Result<Self, mlua::Error> {
        Self::from_lua(value, lua)
    }
}

#[derive(Debug)]
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

fn c<X: IntoLua>(lua: &mlua::Lua) -> impl Fn(X) -> mlua::Result<mlua::Value> + '_ {
    move |x| x.into_lua(lua)
}

fn try_bind<'q, T, X>(
    query: Query<'q, Postgres, PgArguments>,
    value: X,
) -> Result<Query<'q, Postgres, PgArguments>, mlua::Error>
where
    T: 'q + Send + Encode<'q, Postgres> + Type<Postgres> + tealr::ToTypename,
    X: TryInto<T>,
{
    match value.try_into() {
        Ok(x) => Ok(query.bind(x)),
        Err(_) => {
            let from = tealr::type_to_string(&T::to_typename(), false);
            Err(mlua::Error::FromLuaConversionError {
                from: "unknown",
                to: "unknown".into(),
                message: Some(format!(
                    "Can't convert {from} to the required postgresql type"
                )),
            })
        }
    }
}

fn try_json_to_array_of<T: DeserializeOwned>(
    json: serde_json::Value,
) -> Result<Vec<T>, mlua::Error> {
    serde_json::from_value(json).map_err(mlua::Error::external)
}
fn bind_array_of<'a, T>(
    query: Query<'a, Postgres, PgArguments>,
    data: Table,
) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error>
where
    T: 'static + DeserializeOwned + Send + Debug,
    Vec<T>: Encode<'a, Postgres> + Type<Postgres>,
{
    let as_vec: Vec<T> = try_json_to_array_of::<T>(data.0)?;
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
            "DOUBLE PRECISION" | "FLOAT8" => Self::DOUBLE,
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
            "DOUBLE PRECISION[]" | "FLOAT8[]" => Self::DOUBLEArray,
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
    ) -> tealr::mlu::mlua::Result<tealr::mlu::mlua::Value> {
        let v = value.type_info();
        let name = v.name();
        let name = Self::parse_str(name).ok_or(tealr::mlu::mlua::Error::ToLuaConversionError {
            from: v.name().to_string(),
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
                    from: "INTERVAL[]".into(),
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
                .map(|v| {
                    v.iter()
                        .map(|v| l.to_value(v))
                        .collect::<Result<Vec<_>, _>>()
                })
                .map(|v| v.and_then(|v| v.into_lua(l))),

            TypeInformation::Unknown => unreachable!(),
        }
        .map_err(mlua::Error::external)?
    }
    pub fn bind_on<'a>(
        param_type: Option<Input>,
        info: Option<&PgTypeInfo>,
        mut query: Query<'a, Postgres, PgArguments>,
    ) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
        let info = TypeInformation::parse_maybe_str(info.map(TypeInfo::name)).ok_or_else(|| {
            let name = info.map(|v| v.name()).unwrap_or("unknown");
            tealr::mlu::mlua::Error::FromLuaConversionError {
                from: "unknown",
                to: name.to_string(),
                message: Some(format!("Don't know how to convert to {name}")),
            }
        })?;
        query = match (param_type, info) {
            (Some(Input::Boolean(x)), TypeInformation::BOOL | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Integer(x)), TypeInformation::CHARINT) => try_bind::<i8, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::SMALLINT) => try_bind::<i16, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::INT) => try_bind::<i32, _>(query, x)?,
            (Some(Input::Integer(x)), TypeInformation::BIGINT | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Integer(x)), TypeInformation::REAL) => query.bind(x as f32),
            (Some(Input::Number(x)), TypeInformation::REAL) => query.bind(x as f32),
            (Some(Input::Integer(x)), TypeInformation::DOUBLE) => query.bind(x as f64),
            (Some(Input::Number(x)), TypeInformation::DOUBLE | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::String(x)), TypeInformation::VARCHAR | TypeInformation::Unknown) => {
                query.bind(x)
            }
            (Some(Input::Integer(x)), TypeInformation::VARCHAR) => query.bind(x.to_string()),
            //(Some(Input::Number(x)), TypeInformation::VARCHAR) => query.bind(x.0.to_string()),
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
                TypeInformation::INTERVAL => {
                    let x: Interval = Interval::try_from(data)?;
                    query.bind::<PgInterval>(x.into())
                }
                TypeInformation::MONEYArray => {
                    let res = serde_json::from_value::<Vec<i64>>(data.0)
                        .map_err(mlua::Error::external)?
                        .into_iter()
                        .map(PgMoney)
                        .collect::<Vec<_>>();
                    query.bind(res)
                }
                x => {
                    return Err(mlua::Error::FromLuaConversionError {
                        from: "table",
                        to: x.as_lua(),
                        message: Some(format!("going from table to {x:?}")),
                    });
                }
            },
            (x, y) => {
                return Err(mlua::Error::FromLuaConversionError {
                    from: "unknown",
                    to: y.as_lua(),
                    message: Some(format!("going from: {x:?} to {y:?}")),
                });
            }
        };
        Ok(query)
    }
}

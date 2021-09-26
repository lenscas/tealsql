use std::convert::TryInto;

use serde::de::DeserializeOwned;
use sqlx_core::{
    encode::Encode,
    postgres::{types::PgMoney, PgArguments, PgTypeInfo, PgValue, Postgres},
    query::Query,
    type_info::TypeInfo,
    types::Type,
    value::Value,
};
use tealr::mlu::mlua::{self, Integer, LuaSerdeExt, Number, ToLua};
use uuid::Uuid;

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
            println!("Got here?");
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
        let info = info.map(TypeInfo::name);
        let (param_type, info) = dbg!((param_type, info));
        query = match (param_type, info) {
            (Some(Input::bool(x)), Some("BOOL") | None) => query.bind(x),
            (Some(Input::Integer(x)), Some("\"CHAR\"")) => try_bind::<i8, _>(query, x)?,
            (Some(Input::Integer(x)), Some("SMALLINT" | "SMALLSERIAL" | "INT2")) => {
                try_bind::<i16, _>(query, x)?
            }
            (Some(Input::Integer(x)), Some("INT" | "SERIAL" | "INT4")) => {
                try_bind::<i32, _>(query, x)?
            }
            (Some(Input::Integer(x)), Some("BIGINT" | "BIGSERIAL" | "INT8") | None) => {
                query.bind(x)
            }
            (Some(Input::Number(x)), Some("REAL" | "FLOAT4")) => query.bind(x as f32),
            (Some(Input::Number(x)), Some("DOUBLE PRECISION" | "FLOATS") | None) => query.bind(x),
            (Some(Input::String(x)), Some("VARCHAR" | "CHAR" | "TEXT" | "NAME") | None) => {
                query.bind(x)
            }
            (Some(Input::Table(x)), Some("JSON" | "JSONB") | None) => query.bind(x.0),
            (Some(Input::Integer(x)), Some("MONEY")) => query.bind(PgMoney(x)),
            (None, _) => query.bind::<Option<bool>>(None),
            (Some(Input::String(x)), Some("UUID")) => Uuid::parse_str(&x)
                .map_err(mlua::Error::external)
                .map(|v| query.bind(v))?,
            (Some(Input::Table(data)), Some(info)) => match info {
                "BOOL[]" => bind_array_of::<bool>(query, data)?,
                "\"CHAR\"[]" => bind_array_of::<i8>(query, data)?,
                "SMALLINT[]" | "SMALLSERIAL[]" | "INT2[]" => bind_array_of::<i16>(query, data)?,
                "INT[]" | "SERIAL[]" | "INT4[]" => bind_array_of::<i32>(query, data)?,
                "BIGINT[]" | "BIGSERIAL[]" | "INT8[]" => bind_array_of::<i64>(query, data)?,
                "REAL[]" | "FLOAT[]" => bind_array_of::<f32>(query, data)?,
                "DOUBLE PRECISION[]" | "FLOATS[]" => bind_array_of::<f64>(query, data)?,
                "VARCHAR[]" | "CHAR[]" | "TEXT[]" | "NAME[]" => {
                    bind_array_of::<String>(query, data)?
                }
                "JSON[]" | "JSONB[]" => bind_array_of::<serde_json::Value>(query, data)?,
                "MONEY[]" => {
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
            (_, Some(_)) => {
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

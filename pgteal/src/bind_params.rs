use either::Either;
use sqlx::{
    postgres::{PgArguments, PgTypeInfo},
    query::Query,
    Encode, Postgres, Type, TypeInfo,
};
use std::convert::TryInto;

use crate::connection::{Input, QueryParamCollection};

pub(crate) fn bind_params_on<'a, 'b: 'a>(
    params: &'b mut QueryParamCollection,
    info: Either<&[PgTypeInfo], usize>,
    mut query: Query<'a, Postgres, PgArguments>,
) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
    let x = info
        .map_left(|v| v.iter().map(Some))
        .map_right(|n| (0..n).map(|_| None))
        .enumerate()
        .map(|(s, z)| (s as i64, z))
        .map(|(key, info)| (params.remove(&key), info));

    for (lua_type, sql_type_info) in x {
        match bind_on(lua_type, sql_type_info, query) {
            Ok(x) => query = x,
            Err(x) => return Err(x),
        }
    }
    Ok(query)
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
fn bind_on<'a>(
    param_type: Option<Input>,
    info: Option<&PgTypeInfo>,
    mut query: Query<'a, Postgres, PgArguments>,
) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
    let info = info.map(TypeInfo::name);
    query = match (param_type, info) {
        (Some(Input::bool(x)), Some("BOOL") | None) => query.bind(x),
        (Some(Input::Integer(x)), Some("\"CHAR\"")) => try_bind::<i8, _>(query, x)?,
        (Some(Input::Integer(x)), Some("SMALLINT" | "SMALLSERIAL" | "INT2")) => {
            try_bind::<i16, _>(query, x)?
        }
        (Some(Input::Integer(x)), Some("INT" | "SERIAL" | "INT4")) => try_bind::<i32, _>(query, x)?,
        (Some(Input::Integer(x)), Some("BIGINT" | "BIGSERIAL" | "INT8") | None) => query.bind(x),
        (Some(Input::Number(x)), Some("REAL" | "FLOAT4")) => query.bind(x as f32),
        (Some(Input::Number(x)), Some("DOUBLE PRECISION" | "FLOATS") | None) => query.bind(x),
        (
            Some(Input::String(x)),
            Some("VARCHAR" | "CHAR" | "TEXT" | "NAME" | "JSON" | "JSONB") | None,
        ) => query.bind(x),
        (None, _) => query.bind::<Option<bool>>(None),
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

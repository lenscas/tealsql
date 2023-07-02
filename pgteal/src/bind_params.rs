use crate::connection::QueryParamCollection;
use either::Either;
use sqlx::{
    postgres::{PgArguments, PgTypeInfo},
    query::Query,
    Postgres,
};

pub(crate) fn bind_params_on<'a, 'b: 'a>(
    params: &'b mut QueryParamCollection,
    info: Either<&[PgTypeInfo], usize>,
    mut query: Query<'a, Postgres, PgArguments>,
) -> Result<Query<'a, Postgres, PgArguments>, mlua::Error> {
    let x = info
        .map_left(|v| v.iter().map(Some))
        .map_right(|n| (0..n).map(|_| None))
        .enumerate()
        .map(|(s, z)| (s + 1, z))
        .map(|(key, info)| (params.remove(&key), info));

    for (lua_type, sql_type_info) in x {
        match shared::TypeInformation::bind_on(lua_type, sql_type_info, query) {
            Ok(x) => query = x,
            Err(x) => return Err(x),
        }
    }
    Ok(query)
}

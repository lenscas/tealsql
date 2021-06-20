use mlua::ToLua;
use sqlx::{
    postgres::{PgRow, PgValue},
    Column, Row, TypeInfo, Value, ValueRef,
};

pub(crate) struct LuaRow {
    row: PgRow,
}
impl<'lua> mlua::ToLua<'lua> for LuaRow {
    fn to_lua(self, lua: &'lua mlua::Lua) -> std::result::Result<mlua::Value<'lua>, mlua::Error> {
        let columns = self.row.columns();
        let names = columns
            .iter()
            .map(|v| {
                let name = v.name();
                let value = self.row.try_get_raw(name);
                let value = match value {
                    Ok(x) => {
                        if x.is_null() {
                            mlua::Nil
                        } else {
                            decode_value(ValueRef::to_owned(&x), lua)?
                        }
                    }
                    Err(x) => return Err(mlua::Error::external(x)),
                };
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = lua.create_table()?;

        for (k, v) in names {
            table.set(k, v)?;
        }
        Ok(mlua::Value::Table(table))
    }
}

fn c<'lua, X: ToLua<'lua>>(lua: &'lua mlua::Lua) -> impl Fn(X) -> mlua::Result<mlua::Value<'lua>> {
    move |x| x.to_lua(lua)
}

impl From<PgRow> for LuaRow {
    fn from(row: PgRow) -> Self {
        LuaRow { row }
    }
}

fn decode_value(value: PgValue, l: &mlua::Lua) -> mlua::Result<mlua::Value<'_>> {
    let x = value.type_info();
    let name = x.name();
    match name {
        "BOOL" => value.try_decode::<bool>().map(c(l)),
        "CHAR" | "SMALLINT" | "SMALLSERIAL" | "INT2" | "INT" | "SERIAL" | "INT4" | "BIGINT"
        | "BIGSERIAL" | "INT8" => value.try_decode::<i64>().map(c(l)),
        "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOATS" => value.try_decode::<f64>().map(c(l)),
        "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "JSON" | "JSONB" => {
            value.try_decode::<String>().map(c(l))
        }
        "BYTEA" => value.try_decode::<bool>().map(c(l)),
        x => panic!("unsopperted typename: {}", x),
    }
    .map_err(mlua::Error::external)?
}

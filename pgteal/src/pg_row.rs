use shared::Input;
use sqlx::{postgres::PgRow, Column, Row, ValueRef};
use tealr::{mlu::mlua, ToTypename};

pub(crate) struct LuaRow {
    row: PgRow,
}
impl ToTypename for LuaRow {
    fn to_typename() -> tealr::Type {
        tealr::Type::Map(tealr::MapRepresentation {
            key: tealr::Type::new_single("string", tealr::KindOfType::Builtin).into(),
            value: Input::to_typename().into(),
        })
    }
}

impl LuaRow {
    pub fn into_lua_cached(
        self,
        lua: &tealr::mlu::mlua::Lua,
        table: mlua::Table,
    ) -> std::result::Result<mlua::Value, mlua::Error> {
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
                            shared::TypeInformation::decode(ValueRef::to_owned(&x), lua)?
                        }
                    }
                    Err(x) => return Err(mlua::Error::external(x)),
                };
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (k, v) in names {
            table.raw_set(k, v)?;
        }
        Ok(mlua::Value::Table(table))
    }
}

impl tealr::mlu::mlua::IntoLua for LuaRow {
    fn into_lua(
        self,
        lua: &tealr::mlu::mlua::Lua,
    ) -> std::result::Result<mlua::Value, mlua::Error> {
        self.into_lua_cached(lua, lua.create_table()?)
    }
}

impl From<PgRow> for LuaRow {
    fn from(row: PgRow) -> Self {
        LuaRow { row }
    }
}

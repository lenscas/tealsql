use std::borrow::Cow;

use shared::Input;
use sqlx::{postgres::PgRow, Column, Row, ValueRef};
use tealr::{mlu::mlua, TypeName};

pub(crate) struct LuaRow {
    row: PgRow,
}
impl TypeName for LuaRow {
    fn get_type_name(dir: tealr::Direction) -> std::borrow::Cow<'static, str> {
        Cow::Owned(format!("{{string:{}}}", Input::get_type_name(dir)))
    }
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
                            shared::TypeInformation::decode(ValueRef::to_owned(&x), lua)?
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

impl From<PgRow> for LuaRow {
    fn from(row: PgRow) -> Self {
        LuaRow { row }
    }
}

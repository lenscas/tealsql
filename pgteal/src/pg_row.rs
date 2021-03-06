use std::borrow::Cow;

use shared::Input;
use sqlx::{postgres::PgRow, Column, Row, ValueRef};
use tealr::{mlu::mlua, NamePart, TealType, TypeName};

pub(crate) struct LuaRow {
    row: PgRow,
}
impl TypeName for LuaRow {
    fn get_type_parts() -> std::borrow::Cow<'static, [NamePart]> {
        let mut type_parts = vec![
            NamePart::Symbol(Cow::Borrowed("{")),
            NamePart::Type(TealType {
                name: Cow::Borrowed("string"),
                type_kind: tealr::KindOfType::Builtin,
                generics: None,
            }),
            NamePart::Symbol(Cow::Borrowed(":")),
        ];
        type_parts.append(&mut Input::get_type_parts().to_vec());
        type_parts.push(NamePart::Symbol(Cow::Borrowed("}")));
        Cow::Owned(type_parts)
    }
    fn get_type_kind() -> tealr::KindOfType {
        tealr::KindOfType::Builtin
    }
}

impl LuaRow {
    pub fn into_lua_cached<'lua>(
        self,
        lua: &'lua mlua::Lua,
        table: mlua::Table<'lua>,
    ) -> std::result::Result<mlua::Value<'lua>, mlua::Error> {
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

impl<'lua> mlua::ToLua<'lua> for LuaRow {
    fn to_lua(self, lua: &'lua mlua::Lua) -> std::result::Result<mlua::Value<'lua>, mlua::Error> {
        self.into_lua_cached(lua, lua.create_table()?)
    }
}

impl From<PgRow> for LuaRow {
    fn from(row: PgRow) -> Self {
        LuaRow { row }
    }
}

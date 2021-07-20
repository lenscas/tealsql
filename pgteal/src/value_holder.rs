use std::collections::HashMap;

pub(crate) enum ValueHolderOrLuaValue<'lua> {
    Lua(mlua::Value<'lua>),
    Normal(HashMap<i64, ValueHolder>),
}
impl From<HashMap<i64, ValueHolder>> for ValueHolderOrLuaValue<'static> {
    fn from(x: HashMap<i64, ValueHolder>) -> Self {
        Self::Normal(x)
    }
}

impl<'lua> From<mlua::Value<'lua>> for ValueHolderOrLuaValue<'lua> {
    fn from(x: mlua::Value<'lua>) -> Self {
        Self::Lua(x)
    }
}

#[derive(Clone)]
pub(crate) enum ValueHolder {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Nil,
}

impl ValueHolder {
    pub(crate) fn from_lua_value(value: mlua::Value) -> mlua::Result<Self> {
        Ok(match value {
            mlua::Value::Nil => Self::Nil,
            mlua::Value::Boolean(x) => Self::Boolean(x),
            mlua::Value::Integer(x) => Self::Integer(x),
            mlua::Value::Number(x) => Self::Number(x),
            mlua::Value::String(x) => Self::String(x.to_str()?.to_owned()),
            x => {
                return Err(mlua::Error::FromLuaConversionError {
                    from: x.type_name(),
                    to: "bool, number,string",
                    message: Some("Can't store this values in the db".to_string()),
                })
            }
        })
    }
    pub(crate) fn value_to_map(value: mlua::Value) -> mlua::Result<HashMap<i64, Self>> {
        match value {
            mlua::Value::Table(x) => {
                let mut map = HashMap::new();
                for pair in x.pairs::<mlua::Value, mlua::Value>() {
                    let (k, v) = pair?;
                    if let mlua::Value::Integer(k) = k {
                        let value = Self::from_lua_value(v)?;
                        map.insert(k, value);
                    }
                }
                Ok(map)
            }

            _ => Ok(HashMap::new()),
        }
    }
}

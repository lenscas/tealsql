use std::{ffi::OsString, fmt::Display, path::Path, slice::from_ref};

use anyhow::Context;
use inflector::Inflector;
use sqlx::{postgres::PgTypeInfo, Column, Executor, Pool, Postgres, TypeInfo};
use tealr::TypeName;

use crate::sql_parser::ParsedSql;

#[derive(Clone)]
pub(crate) struct TealParts {
    pub(crate) container_name: String,
    pub(crate) functions: Vec<String>,
    pub(crate) input_type: StructAndName,
    pub(crate) output_type: StructAndName,
}

fn display_params(params: &[String]) -> String {
    params
        .iter()
        .map(ToOwned::to_owned)
        .enumerate()
        .map(|(key, v)| (key + 1, v))
        .map(|(key, v)| format!("\t${key} = {v}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) async fn query_to_teal(
    pool: Pool<Postgres>,
    parsed_query: ParsedSql,
) -> Result<TealParts, anyhow::Error> {
    let x = pool.describe(&parsed_query.sql).await.with_context(|| {
        format!(
            "In query: `{}`\nSQL:\n{}\n\nParameters ({}) :\n{}",
            parsed_query.name,
            parsed_query.sql.trim(),
            parsed_query.params.len(),
            display_params(&parsed_query.params)
        )
    })?;

    let iter = x
        .columns()
        .iter()
        .map(|v| Ok((v.name(), std::slice::from_ref(v.type_info()))));
    let return_type = create_struct_from_db(iter, &parsed_query.name, "Out")?;

    let desc = x.parameters();
    let iter = desc
        .into_iter()
        .map(|v| v.left_or_else(|v| panic!("Expected type info, got number: {}", v)))
        .filter(|v| !v.iter().all(|v| v.is_void() || v.is_null()))
        .flat_map(|v| v.iter())
        .enumerate()
        .map(|(key, pg_type)| {
            parsed_query
                .params
                .get(key)
                .ok_or_else(
                    || {
                            let parameter_list = display_params(&parsed_query.params);
                            anyhow::anyhow!(
                                "Query `{}` did not contain enough named parameters.\nNeeded at least {} parameters. Found {} parameters.\nParameters:\n{}\nsql:{}\n\nNote: This can be caused by using `$` directly inside the query. Use `:name` instead to bind parameters.",
                                parsed_query.name,
                                key + 1,
                                parsed_query.params.len(),
                                parameter_list,
                                parsed_query.sql
                            )
                    }
                )
                .map(|name| (name.as_str(), from_ref(pg_type)))
        });
    let input_type = create_struct_from_db(iter, &parsed_query.name, "In")?;

    let fetch_all = parsed_query
        .create_fetch_all
        .then(|| {
            make_function(
                &parsed_query,
                &input_type.name,
                &return_type.name,
                PossibleFunctions::FetchAll,
            )
        })
        .unwrap_or_default();
    let execute = parsed_query
        .create_execute
        .then(|| {
            make_function(
                &parsed_query,
                &input_type.name,
                &return_type.name,
                PossibleFunctions::Execute,
            )
        })
        .unwrap_or_default();
    let fetch_one = parsed_query
        .create_fetch_one
        .then(|| {
            make_function(
                &parsed_query,
                &input_type.name,
                &return_type.name,
                PossibleFunctions::FetchOne,
            )
        })
        .unwrap_or_default();
    let fetch_optional = parsed_query
        .create_fetch_optional
        .then(|| {
            make_function(
                &parsed_query,
                &input_type.name,
                &return_type.name,
                PossibleFunctions::FetchOptional,
            )
        })
        .unwrap_or_default();
    Ok(TealParts {
        container_name: parsed_query.name,
        functions: [fetch_all, fetch_one, fetch_optional, execute]
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect(),
        input_type,
        output_type: return_type,
    })
}

#[derive(Clone, Copy)]
enum PossibleFunctions {
    FetchOptional,
    FetchAll,
    Execute,
    FetchOne,
}

impl Display for PossibleFunctions {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let fn_name = match self {
            PossibleFunctions::FetchOptional => "fetch_optional",
            PossibleFunctions::FetchAll => "fetch_all",
            PossibleFunctions::Execute => "execute",
            PossibleFunctions::FetchOne => "fetch_one",
        };
        fn_name.fmt(fmt)
    }
}

impl PossibleFunctions {
    fn return_single(self) -> bool {
        match self {
            Self::FetchAll => false,
            Self::FetchOptional | Self::FetchOne | Self::Execute => true,
        }
    }
    fn return_digit(self) -> bool {
        match self {
            Self::Execute => true,
            Self::FetchAll | Self::FetchOne | Self::FetchOptional => false,
        }
    }
}

fn make_function(
    query: &ParsedSql,
    input_type: &str,
    return_type: &str,
    function_type: PossibleFunctions,
) -> String {
    let return_name = match (function_type.return_digit(), function_type.return_single()) {
        (true, true) => "integer".to_owned(),
        (true, false) => "{integer}".to_owned(),
        (false, true) => return_type.to_owned(),
        (false, false) => format!("{{{}}}", return_type),
    };
    let function_header = format!(
        "        {} = function (params: {}, connection: libpgteal.Connection): {}",
        function_type, input_type, return_name
    );
    let params: String = query
        .params
        .iter()
        .map(|v| "                \"".to_owned() + v + "\"")
        .collect::<Vec<_>>()
        .join(",\n");
    let params = format!(
        "local param_order:{{string}} = {{\n{}\n            }}",
        params
    );
    format!(
        "{}
            {}
            local query_params = {{}}
            for k,v in ipairs(param_order) do
                query_params[k] = (params as {{string:{}}})[v]
            end
            return connection:{}(
[[{}\n]],
                query_params
            ) as {}
        end",
        function_header,
        params,
        shared::Input::get_type_name(tealr::Direction::ToLua),
        function_type,
        query.sql,
        return_name
    )
}

#[derive(Clone)]
pub(crate) struct StructAndName {
    pub(crate) name: String,
    pub(crate) written_struct: String,
}

fn create_struct_from_db<
    'a,
    'b,
    X: Iterator<Item = Result<(&'a str, &'b [PgTypeInfo]), anyhow::Error>>,
>(
    fields: X,
    name: &str,
    attached: &str,
) -> Result<StructAndName, anyhow::Error> {
    let full_name = name.to_pascal_case() + attached;
    let fields = fields
        .map(|res| match res {
            Err(x) => Err(x),
            Ok((key, teal_type)) => Ok(format!(
                "{} : {}",
                key,
                teal_type
                    .iter()
                    .map(|v| v.name())
                    .filter_map(shared::TypeInformation::parse_str)
                    .map(|v| v.as_lua())
                    .collect::<Vec<_>>()
                    .join(" | ")
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let written_struct = if fields.is_empty() {
        format!("local type {} = nil", full_name)
    } else {
        let return_type = "    ".to_string() + &fields.join("\n    ");
        format!("local type {} = record \n{}\nend", full_name, return_type)
    };
    Ok(StructAndName {
        name: full_name,
        written_struct,
    })
}

pub(crate) fn write_to_file(
    original_file_path: &Path,
    teal_pattern: &str,
    parts: Vec<TealParts>,
) -> Result<(), anyhow::Error> {
    let glued_types = parts
        .iter()
        .map(|v| {
            [
                v.input_type.written_struct.clone(),
                v.output_type.written_struct.clone(),
            ]
        })
        .flat_map(std::array::IntoIter::new)
        .collect::<Vec<_>>()
        .join("\n");

    let modules = parts
        .into_iter()
        .map(|part| {
            let reexported = format!(
                "    {} = {{\n        {} = {},\n        {}= {},\n",
                part.container_name,
                part.input_type.name,
                part.input_type.name,
                part.output_type.name,
                part.output_type.name
            );
            let functions = part.functions.join(",\n");
            reexported + &functions + "\n    }"
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let path = get_path(teal_pattern, original_file_path)?;
    let path = Path::new(&path);

    std::fs::create_dir_all(path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "Could not create directories needed. Path `{}` has no parent",
            path.to_string_lossy()
        )
    })?)
    .with_context(|| {
        format!(
            "While creating the directories for {}",
            path.to_string_lossy()
        )
    })?;

    let to_write = format!(
        "local libpgteal = require(\"libpgteal\")\n{}\nreturn {{\n{}\n}}",
        glued_types, modules
    );
    std::fs::write(path, to_write)
        .with_context(|| format!("While writing to {}", path.to_string_lossy()))?;

    Ok(())
}

#[derive(PartialEq)]
enum PathParsingState {
    Nothing,
    StartPattern,
    MakingPattern(String),
}

fn get_path(path_template: &str, file_path: &Path) -> Result<OsString, anyhow::Error> {
    let mut end_path = OsString::new();
    let mut state = PathParsingState::Nothing;
    for c in path_template.chars() {
        match (c, &mut state) {
            ('{', PathParsingState::Nothing) => state = PathParsingState::StartPattern,
            ('{', PathParsingState::StartPattern) => {
                state = PathParsingState::Nothing;
                end_path.push(c.to_string());
            }
            (x, PathParsingState::StartPattern) => {
                state = PathParsingState::MakingPattern(x.to_string());
            }
            ('}', PathParsingState::MakingPattern(x)) => {
                if x == "dir" {
                    let path = file_path
                        .parent()
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(Default::default);
                    end_path.push(&path);
                } else if x == "name" {
                    end_path.push(
                        &file_path
                            .file_stem()
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(OsString::new),
                    )
                } else if x == "ext" {
                    end_path.push(
                        &file_path
                            .extension()
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(OsString::new),
                    )
                } else {
                    return Err(anyhow::anyhow!("{} is not a valid pattern name", x));
                }
                state = PathParsingState::Nothing;
            }
            (c, PathParsingState::MakingPattern(x)) => {
                x.push(c);
                state = PathParsingState::MakingPattern(x.clone())
            }
            (x, PathParsingState::Nothing) => end_path.push(x.to_string()),
        }
        if c == '{' && state == PathParsingState::Nothing {
            state = PathParsingState::StartPattern
        }
    }
    Ok(end_path)
}

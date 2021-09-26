use std::{convert::TryInto, ffi::OsString, fmt::Display, path::Path};

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

pub(crate) async fn query_to_teal(pool: Pool<Postgres>, parsed_query: ParsedSql) -> TealParts {
    let x = pool
        .describe(&parsed_query.sql)
        .await
        .expect("Could not describe sql");
    let iter = x
        .columns()
        .iter()
        .map(|v| (v.name(), std::slice::from_ref(v.type_info())));
    let return_type = create_struct_from_db(iter, &parsed_query.name, "Out");

    let desc = x.parameters();
    let iter = desc
        .iter()
        .map(|v| v.left_or_else(|v| panic!("Expected type info, got number: {}", v)))
        .filter(|v| !v.iter().all(|v| v.is_void() || v.is_null()))
        .enumerate()
        .map(|(key, pg_type)| {
            let name = parsed_query.params.get(key).unwrap_or_else(|| {
                panic!(
                    "The query needs more parameter than have been provided? Needed: {}, got: {}",
                    key,
                    parsed_query.params.len()
                )
            });
            (name.as_str(), pg_type)
        });
    let input_type = create_struct_from_db(iter, &parsed_query.name, "In");

    let fetch_all = make_function(
        &parsed_query,
        &input_type.name,
        &return_type.name,
        PossibleFunctions::FetchAll,
    );
    let execute = make_function(
        &parsed_query,
        &input_type.name,
        &return_type.name,
        PossibleFunctions::Execute,
    );
    let fetch_one = make_function(
        &parsed_query,
        &input_type.name,
        &return_type.name,
        PossibleFunctions::FetchOne,
    );
    let fetch_optional = make_function(
        &parsed_query,
        &input_type.name,
        &return_type.name,
        PossibleFunctions::FetchOptional,
    );
    TealParts {
        container_name: parsed_query.name,
        functions: vec![fetch_all, fetch_one, fetch_optional, execute],
        input_type,
        output_type: return_type,
    }
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
        "        {} = function (params: {}, connection: Connection): {}",
        function_type, input_type, return_name
    );
    let params: String = query
        .params
        .iter()
        .map(|v| "    \"".to_owned() + v + "\"")
        .collect::<Vec<_>>()
        .join(",\n");
    let params = format!("local param_order:{{string}} = {{\n{}\n}}", params);
    format!(
        "          {}
		{}
		local query_params = {{}}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {{string:{}}})[v]
		end
		return connection:{}(
			[[{}]],
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

fn create_struct_from_db<'a, 'b, X: Iterator<Item = (&'a str, &'b [PgTypeInfo])>>(
    fields: X,
    name: &str,
    attached: &str,
) -> StructAndName {
    let full_name = name.to_pascal_case() + attached;
    let fields = fields
        .map(|(key, teal_type)| {
            format!(
                "{} : {}",
                key,
                teal_type
                    .iter()
                    .map(|v| v.name())
                    .filter_map(shared::TypeInformation::parse_str)
                    .map(|v| v.as_lua())
                    .collect::<Vec<_>>()
                    .join(" | ")
            )
        })
        .collect::<Vec<_>>();
    let written_struct = if fields.is_empty() {
        format!("local type {} = nil", full_name)
    } else {
        let return_type = "    ".to_string() + &fields.join("\n    ");
        format!("local type {} = record \n{}\nend", full_name, return_type)
    };
    StructAndName {
        name: full_name,
        written_struct,
    }
}

pub(crate) fn write_to_file(
    original_file_path: &Path,
    teal_pattern: &str,
    parts: Vec<TealParts>,
) -> std::io::Result<()> {
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
            let functions = part.functions.join(",\n            ");
            reexported + &functions + "}"
        })
        .collect::<Vec<_>>()
        .join(",");

    let path = get_path(teal_pattern, original_file_path);
    let path = Path::new(&path);

    std::fs::create_dir_all(path.parent().unwrap())?;

    let to_write = format!(
        "local Connection = require(\"libpgteal\").Connection\n{}\nreturn {{{}}}",
        glued_types, modules
    );
    std::fs::write(path, to_write)?;

    Ok(())
}

#[derive(PartialEq)]
enum PathParsingState {
    Nothing,
    StartPattern,
    MakingPattern(String),
}

fn get_path(path_template: &str, file_path: &Path) -> OsString {
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
                            .unwrap_or_else(|| "".try_into().unwrap()),
                    )
                } else if x == "ext" {
                    end_path.push(
                        &file_path
                            .extension()
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| "".try_into().unwrap()),
                    )
                } else {
                    panic!("`{}` is not a valid pattern", x)
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
    end_path
}

use std::{error::Error, fs::read_to_string, path::Path};

use sqlx::{
    pool::Pool,
    postgres::{PgTypeInfo, Postgres},
    Column, Executor, TypeInfo,
};

use inflector::Inflector;

pub(crate) struct ParsedSql {
    pub sql: String,
    pub name: String,
    pub params: Vec<String>,
}

pub(crate) fn parse_sql_file(file: &Path) -> Result<Vec<ParsedSql>, Box<dyn Error>> {
    let res = read_to_string(file)?;

    let possibles = dbg!(res.split(";\n").collect::<Vec<_>>());

    let parsed_sql = possibles
        .into_iter()
        .map(|possible| {
            let mut split = possible.split("*/");
            let first_part = split.next().expect("Could not find name comment");
            let name = first_part
                .split("/* @name=")
                .nth(1)
                .map(|v| v.trim().to_owned())
                .expect("could not find name comment");
            let rest_of_query = split
                .map(ToOwned::to_owned)
                .map(|mut v| {
                    v.push_str("*/");
                    v
                })
                .collect::<String>()
                .replacen("*/", "", 1);

            let params = rest_of_query
                .chars()
                .fold(
                    (false, Vec::new()),
                    |(is_reading_params, mut params): (bool, Vec<String>), current_char| {
                        if is_reading_params {
                            if current_char.is_whitespace() || current_char == ';' {
                                (false, params)
                            } else {
                                match params.last_mut() {
                                    Some(x) => {
                                        x.push(current_char);
                                    }
                                    None => {
                                        params.push(current_char.to_string());
                                    }
                                };
                                (true, params)
                            }
                        } else {
                            (current_char == ':', params)
                        }
                    },
                )
                .1;
            let query = {
                let mut query = rest_of_query;

                params
                    .iter()
                    .map(|v| format!(":{}", v))
                    .enumerate()
                    .for_each(|(at, name)| query = query.replace(&name, &format!("${}", at + 1)));
                query
            };
            println!("name:{}\nquery:{}\nparams:{:?}\n----", name, query, params);
            ParsedSql {
                sql: query,
                name,
                params,
            }
        })
        .collect();

    Ok(parsed_sql)
}
pub(crate) struct TealParts {
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

    let function_header = format!(
        "{}_all = function (params: {}, connection: Connection): {{{}}}",
        parsed_query.name, input_type.name, return_type.name
    );
    let function_body = {
        let params: String = parsed_query
            .params
            .iter()
            .map(|v| "    \"".to_owned() + v + "\"")
            .collect::<Vec<_>>()
            .join(",\n");
        let params = format!("local param_order:{{string}} = {{\n{}\n}}", params);
        format!(
            "      {}
        local query_params = {{}}
        for k,v in ipairs(param_order) do
            query_params[k] = (params as {{string:any}})[v]
        end
        return connection:fetch_all([[{}]],query_params) as {{{}}}",
            params, parsed_query.sql, return_type.name
        )
    };

    let function = format!("{}\n{}\nend", function_header, function_body);
    TealParts {
        functions: vec![function],
        input_type,
        output_type: return_type,
    }
}

pub fn sql_type_to_teal(type_name: &str) -> &'static str {
    match type_name {
        "BOOL" => "boolean",
        "CHAR" | "SMALLINT" | "SMALLSERIAL" | "INT2" | "INT" | "SERIAL" | "INT4" | "BIGINT"
        | "BIGSERIAL" | "INT8" => "integer",
        "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOATS" => "number",
        "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "JSON" | "JSONB" => "string",
        "BYTEA" => "Array<integer>",
        x => panic!("unsopperted typename: {}", x),
    }
}

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
                    .map(|v| sql_type_to_teal(v.name()))
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

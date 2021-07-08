mod app;
mod sql_parser;

use glob::glob;
use sqlx::postgres::PgPool;

use crate::{
    app::{get_app, Params},
    sql_parser::{parse_sql_file, query_to_teal},
};

#[async_std::main]
async fn main() {
    let Params {
        teal_pattern,
        sql_pattern,
        connection_string,
    } = get_app();

    let pool = PgPool::connect(&connection_string)
        .await
        .unwrap_or_else(|x| panic!("Could not connect to the DB. Error:\n{}", x));

    let x = glob(&sql_pattern).unwrap_or_else(|err| {
        panic!("Error in pattern. Error:\n{}", err);
    });
    let mut functions = Vec::new();
    let mut types = Vec::new();
    for file in x {
        match file {
            Ok(file) => {
                let parsed_sql = parse_sql_file(&file).unwrap();
                for parsed_query in parsed_sql {
                    let res = query_to_teal(pool.clone(), parsed_query).await;
                    functions.push(res.functions);
                    types.push(res.input_type);
                    types.push(res.output_type);
                }
            }
            Err(x) => panic!("Error with file. Error:\n{}", x),
        }
    }
    let glued_types = types
        .iter()
        .map(|v| v.written_struct.clone())
        .collect::<Vec<_>>()
        .join("\n");
    let types_to_rexport = types
        .into_iter()
        .map(|v| format!("   {} = {}", v.name, v.name))
        .collect::<Vec<_>>()
        .join(",\n");
    let glued_functions = functions
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(",\n\n");
    println!(
        "{}\nreturn {{\n{},\n{}\n}}",
        glued_types, types_to_rexport, glued_functions
    )
}

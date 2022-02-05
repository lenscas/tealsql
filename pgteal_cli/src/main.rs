mod app;
mod sql_parser;
mod tl_generator;

use glob::glob;
use sqlx::postgres::PgPool;

use crate::{
    app::{get_app, Params},
    sql_parser::parse_sql_file,
    tl_generator::query_to_teal,
};

#[tokio::main]
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

    for file in x {
        let mut parsed = Vec::new();
        let file = match file {
            Ok(file) => {
                let parsed_sql = parse_sql_file(&file).unwrap();
                for parsed_query in parsed_sql {
                    parsed.push(query_to_teal(pool.clone(), parsed_query).await);
                }
                file
            }
            Err(x) => panic!("Error with file. Error:\n{}", x),
        };
        tl_generator::write_to_file(file.as_path(), &teal_pattern, parsed).unwrap();
    }
}

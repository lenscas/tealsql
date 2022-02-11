mod app;
mod sql_parser;
mod tl_generator;

use anyhow::Context;
use glob::glob;
use sqlx::postgres::PgPool;

use crate::{
    app::{get_app, Params},
    sql_parser::parse_sql_file,
    tl_generator::query_to_teal,
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Params {
        teal_pattern,
        sql_pattern,
        connection_string,
    } = match get_app()? {
        app::Action::ParseFiles(x) => x,
        app::Action::PrintConfig(x) => {
            println!("{}", toml::to_string_pretty(&x)?);
            return Ok(());
        }
    };

    let pool = PgPool::connect(&connection_string).await?;

    let x = glob(&sql_pattern)?;

    for file in x {
        let mut parsed = Vec::new();
        let file = match file {
            Ok(file) => {
                let parsed_sql = parse_sql_file(&file)?;
                for parsed_query in parsed_sql {
                    parsed.push(
                        query_to_teal(pool.clone(), parsed_query)
                            .await
                            .with_context(|| format!("In File: {}", file.to_string_lossy()))?,
                    );
                }
                file
            }
            Err(x) => return Err(x.into()),
        };
        tl_generator::write_to_file(file.as_path(), &teal_pattern, parsed)?;
    }
    Ok(())
}

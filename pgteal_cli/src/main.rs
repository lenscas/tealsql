mod app;
mod sql_parser;
mod tl_generator;

use std::collections::HashMap;

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
                let mut type_collection = Default::default();
                for parsed_query in parsed_sql {
                    let res = query_to_teal(pool.clone(), parsed_query)
                        .await
                        .with_context(|| format!("In File: {}", file.to_string_lossy()))?;
                    type_collection = insert_type_def_into_collection(
                        type_collection,
                        &res.parts.input_type.name,
                        res.input_type_defs,
                    )
                    .with_context(|| {
                        format!(
                            "In File {}, query: {}",
                            file.to_string_lossy(),
                            res.parts.container_name
                        )
                    })?;
                    type_collection = insert_type_def_into_collection(
                        type_collection,
                        &res.parts.output_type.name,
                        res.output_type_defs,
                    )
                    .with_context(|| {
                        format!(
                            "In File {}, query: {}",
                            file.to_string_lossy(),
                            res.parts.container_name
                        )
                    })?;
                    parsed.push(res.parts);
                }
                file
            }
            Err(x) => return Err(x.into()),
        };
        tl_generator::write_to_file(file.as_path(), &teal_pattern, parsed)?;
    }
    Ok(())
}

fn display_type_defs(def: &HashMap<String, String>) -> String {
    let mut to_sort = def.iter().collect::<Vec<_>>();
    to_sort.sort();
    to_sort
        .into_iter()
        .map(|(key, value)| format!("\t{key} = {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn insert_type_def_into_collection(
    mut type_collection: HashMap<String, HashMap<String, String>>,
    name: &str,
    typedef: HashMap<String, String>,
) -> Result<HashMap<String, HashMap<String, String>>, anyhow::Error> {
    let is_equal = type_collection.get(name).map(|v| {
        v.len() == typedef.len()
            && v.iter().all(|(key, value)| {
                typedef
                    .get(key)
                    .map(|type_in_def| {
                        if type_in_def == value {
                            true
                        } else {
                            println!("{type_in_def} != {value}");
                            false
                        }
                    })
                    .unwrap_or(false)
            })
    });

    match is_equal {
        Some(true) => Ok(type_collection),
        Some(false) => Err(anyhow::anyhow!(
            "Conflicting definition for: {name}.\nExisting definition:\n{}\nConflicts with:\n{}",
            display_type_defs(type_collection.get(name).unwrap()),
            display_type_defs(&typedef)
        )),
        None => {
            type_collection.insert(name.to_string(), typedef);
            Ok(type_collection)
        }
    }
}

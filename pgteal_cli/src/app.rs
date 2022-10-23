use std::fs::read_to_string;

use clap::{App, Arg};
use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Serialize)]
pub struct ConfigFile {
    connection_string: Option<String>,
    sql_pattern: Option<String>,
    teal_pattern: Option<String>,
}
pub struct Params {
    pub teal_pattern: String,
    pub sql_pattern: String,
    pub connection_string: String,
}

pub enum Action {
    ParseFiles(Params),
    PrintConfig(ConfigFile),
}

pub fn get_app() -> Result<Action, anyhow::Error> {
    let matches = App::new("PG Teal")
        .version("0.0")
        .author("Lenscas <lenscas@gmail.com>")
        .about("Generates teal types and functions based on sql files.")
        .arg(
            Arg::with_name("connection_string")
                .short("c")
                .long("connection")
                .help("The connection string to connect to the DB")
                .takes_value(true)
                .value_name("CONNECTION"),
        )
        .arg(
            Arg::with_name("sql_file_pattern")
                .short("s")
                .long("sqlPattern")
                .help("The pattern used to find sql files")
                .takes_value(true)
                .value_name("PATTERN"),
        )
        .arg(
            Arg::with_name("teal_file_pattern")
                .short("t")
                .long("tealPattern")
                .help("The pattern used to name teal files")
                .takes_value(true)
                .value_name("PATTERN"),
        )
        .arg(
            Arg::with_name("config_path")
                .short("f")
                .long("config")
                .help("The path of the config file")
                .takes_value(true)
                .value_name("CONFIG_PATH")
                .default_value("./db_config.toml"),
        )
        .arg(
            Arg::with_name("print_default_config")
                .long("printDefaultConfig")
                .help("Only print the default config file")
                .takes_value(false),
        ).after_help("
sqlPattern: 
\tUses normal glob patterns.
    
\tExample: `./src/**/*.sql` finds every file that ends with `.sql` and is inside `./src/` or its child directories

tealPattern:
\t{name} = file name without extension of the sql file use to generate this teal file.
\t{dir} = directory of the sql file used to generate this teal file.
\t{ext} = file extension of the sql file used to generate this teal file.

\tExample: `{dir}/{name}_{ext}.tl` will place the teal files next to the sql files used to generate them.
        ")
        .get_matches();
    if matches.args.get("print_default_config").is_some() {
        return Ok(Action::PrintConfig(ConfigFile {
            teal_pattern: Some("{dir}/{name}_{ext}.tl".to_string()),
            sql_pattern: Some("./src/**/*.sql".to_string()),
            connection_string: Some("postgres://userName:password@host/database".to_string()),
        }));
    }
    let config: ConfigFile = matches
        .args
        .get("config_path")
        .and_then(|v| v.vals.get(0))
        .and_then(|v| read_to_string(v).ok().map(|v| toml::from_str(&v)))
        .unwrap_or_else(|| Ok(Default::default()))?;

    let teal_pattern = matches
        .args
        .get("teal_file_pattern")
        .map(|v| v.vals[0].to_str().unwrap().to_owned())
        .or(config.teal_pattern)
        .ok_or_else(|| {
            anyhow::anyhow!("--tealPattern not provided nor teal_pattern set in config")
        })?;
    let sql_pattern = matches
        .args
        .get("sql_file_pattern")
        .map(|v| v.vals[0].to_str().unwrap().to_owned())
        .or(config.sql_pattern)
        .ok_or_else(|| {
            anyhow::anyhow!("--sqlPattern not provided nor sql_pattern set in config")
        })?;
    let connection_string = matches
        .args
        .get("connection_string")
        .map(|v| v.vals[0].to_str().unwrap().to_owned())
        .or(config.connection_string)
        .ok_or_else(|| {
            anyhow::anyhow!("--connection not provided nor connection_string set in config")
        })?;

    Ok(Action::ParseFiles(Params {
        teal_pattern,
        sql_pattern,
        connection_string,
    }))
}

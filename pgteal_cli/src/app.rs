use clap::{App, Arg};

pub struct Params {
    pub teal_pattern: String,
    pub sql_pattern: String,
    pub connection_string: String,
}
pub fn get_app() -> Params {
    let matches = App::new("PG Teal")
        .version("0.0")
        .author("Lenscas <lenscas@gmail.com>")
        .about("Generates teal types and functions based on sql.")
        .arg(
            Arg::with_name("connection_string")
                .short("c")
                .long("connection")
                .help("The connection string to connect to the DB")
                .takes_value(true)
                .value_name("CONNECTION")
                .required(true),
        )
        .arg(
            Arg::with_name("sql_file_pattern")
                .short("s")
                .long("sqlPattern")
                .help("The pattern used to find sql files")
                .takes_value(true)
                .value_name("PATTERN")
                .required(true),
        )
        .arg(
            Arg::with_name("teal_file_pattern")
                .short("t")
                .long("tealPattern")
                .help("The pattern used to name teal files")
                .takes_value(true)
                .value_name("PATTERN")
                .required(true),
        )
        .get_matches();
    let teal_pattern = matches.args.get("teal_file_pattern").unwrap().vals[0]
        .to_str()
        .unwrap()
        .to_owned();
    let sql_pattern = matches.args.get("sql_file_pattern").unwrap().vals[0]
        .to_str()
        .unwrap()
        .to_owned();
    let connection_string = matches.args.get("connection_string").unwrap().vals[0]
        .to_str()
        .unwrap()
        .to_owned();
    Params {
        teal_pattern,
        sql_pattern,
        connection_string,
    }
}

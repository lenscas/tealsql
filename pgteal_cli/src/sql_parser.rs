use std::{error::Error, fs::read_to_string, path::Path};

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

            let mut params = rest_of_query
                .chars()
                .fold(
                    (false, Vec::new()),
                    |(is_reading_params, mut params): (bool, Vec<String>), current_char| {
                        if is_reading_params {
                            if current_char.is_whitespace() || current_char == ';' {
                                params.push("".to_string());
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
            let param = params.pop();
            if let Some(x) = param {
                if !x.is_empty() {
                    params.push(x);
                }
            }
            let query = {
                let mut query = rest_of_query;

                params
                    .iter()
                    .map(|v| format!(":{}", v))
                    .enumerate()
                    .for_each(|(at, name)| query = query.replace(&name, &format!("${}", at + 1)));
                query
            };
            ParsedSql {
                sql: query,
                name,
                params,
            }
        })
        .collect();

    Ok(parsed_sql)
}

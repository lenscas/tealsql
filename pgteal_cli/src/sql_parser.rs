use std::{collections::HashMap, error::Error, fs::read_to_string, path::Path};

pub(crate) struct ParsedSql {
    pub sql: String,
    pub name: String,
    pub params: Vec<String>,
}

#[derive(Debug, Clone)]
enum ParserState {
    Searching,
    ParseCommentPart,
    InConfigComment,
    ParsingName(String),
    SearchingSeparator(String),
    SearchingValue(String),
    ParsingValue { name: String, value: String },
    ParseEndComment,
    SearchingParamStart { in_string: bool, escape_next: bool },
    FoundParam { param_name: String, in_string: bool },
}
fn create_error(
    got_char: char,
    expected: &[&str],
    at_line: u32,
    at_char: u32,
    state: &ParserState,
    debug: bool,
) -> ParserState {
    panic!(
        "Expected `{}`. Got {}. AtLine: {}, character: {}. {}",
        expected.join(" , "),
        got_char,
        at_line,
        at_char,
        if debug {
            format!("State: {:?}", state)
        } else {
            "".to_string()
        }
    );
}
fn try_construct_parsed_sql(
    mut config: HashMap<String, String>,
    sql: String,
    params: Vec<String>,
) -> ParsedSql {
    let name = config.remove("name").unwrap_or_else(|| {
        panic!(
            "Config for query did not contain name. Found config: {}",
            config
                .iter()
                .map(|(k, v)| format!("{k} = {v}\n"))
                .collect::<String>()
        )
    });
    ParsedSql { name, sql, params }
}
pub(crate) fn parse_sql_file(file: &Path) -> Result<Vec<ParsedSql>, Box<dyn Error>> {
    let res = read_to_string(file)?;
    let mut state = ParserState::Searching;
    let mut at_line = 1;
    let mut at_char = 0;
    let mut found_queries: Vec<ParsedSql> = Vec::new();
    let mut query_config = HashMap::new();
    let mut query_params = Vec::new();
    let mut qeuery_sql = String::new();
    let mut query_param_count: u32 = 0;
    for char in res.chars() {
        at_char += 1;
        if char == '\n' {
            at_line += 1;
            at_char = 0;
        }
        let current_state = state.clone();
        let create_error =
            move |expected| create_error(char, expected, at_line, at_char, &current_state, true);
        state = match state {
            ParserState::Searching => {
                if char == '/' {
                    ParserState::ParseCommentPart
                } else if char == '*' {
                    ParserState::ParseEndComment
                } else if char.is_whitespace() {
                    ParserState::Searching
                } else {
                    create_error(&["/"])
                }
            }
            ParserState::ParseCommentPart => {
                if char == '*' {
                    ParserState::InConfigComment
                } else {
                    create_error(&["*"])
                }
            }
            ParserState::InConfigComment => {
                if char == '@' {
                    ParserState::ParsingName(String::new())
                } else {
                    ParserState::InConfigComment
                }
            }
            ParserState::ParsingName(x) => {
                if char.is_alphanumeric() {
                    ParserState::ParsingName(x + &char.to_string())
                } else if char == '=' {
                    ParserState::SearchingValue(x)
                } else if char.is_whitespace() {
                    ParserState::SearchingSeparator(x)
                } else {
                    create_error(&["name", "whitespace", "="])
                }
            }
            ParserState::SearchingSeparator(x) => {
                if char == '=' {
                    ParserState::SearchingValue(x)
                } else if char.is_whitespace() {
                    ParserState::SearchingSeparator(x)
                } else {
                    create_error(&["=", "whitespace"])
                }
            }
            ParserState::SearchingValue(x) => {
                if char.is_alphanumeric() || char == '_' {
                    ParserState::ParsingValue {
                        name: x,
                        value: char.to_string(),
                    }
                } else if char.is_whitespace() {
                    ParserState::SearchingValue(x)
                } else {
                    create_error(&["alphanumeric", "_", "whitespace"])
                }
            }
            ParserState::ParsingValue { name, value } => {
                if char.is_alphanumeric() || char == '_' {
                    ParserState::ParsingValue {
                        name,
                        value: value + &char.to_string(),
                    }
                } else if char.is_whitespace() {
                    if query_config.contains_key(&name) {
                        panic!("Duplicate key {}, duplicate at line: {at_line}", name)
                    }
                    query_config.insert(name, value);
                    ParserState::Searching
                } else if char == '*' {
                    ParserState::ParseEndComment
                } else {
                    create_error(&["alphanumeric", "_", "whitespace", "*"])
                }
            }
            ParserState::ParseEndComment => {
                if char == '/' {
                    ParserState::SearchingParamStart {
                        in_string: false,
                        escape_next: false,
                    }
                } else {
                    create_error(&["/"])
                }
            }
            ParserState::SearchingParamStart {
                in_string,
                escape_next,
            } => {
                if escape_next {
                    if char == ':' {
                        qeuery_sql.push(':');
                    } else if char == '\\' {
                        qeuery_sql.push('\\');
                    } else {
                        qeuery_sql.push('\\');
                        qeuery_sql.push(char);
                    }
                    ParserState::SearchingParamStart {
                        in_string,
                        escape_next: false,
                    }
                } else if char == '\\' {
                    ParserState::SearchingParamStart {
                        in_string,
                        escape_next: true,
                    }
                } else if char == ';' {
                    if in_string {
                        qeuery_sql.push(';');
                        ParserState::SearchingParamStart {
                            in_string,
                            escape_next,
                        }
                    } else {
                        qeuery_sql.push(';');
                        found_queries.push(try_construct_parsed_sql(
                            query_config,
                            qeuery_sql,
                            query_params,
                        ));
                        query_config = Default::default();
                        qeuery_sql = Default::default();
                        query_params = Default::default();
                        query_param_count = 0;
                        ParserState::Searching
                    }
                } else if char == ':' {
                    query_param_count += 1;
                    qeuery_sql.push('$');
                    qeuery_sql.push_str(&query_param_count.to_string());
                    ParserState::FoundParam {
                        in_string,
                        param_name: String::new(),
                    }
                } else if char == '\'' {
                    ParserState::SearchingParamStart {
                        escape_next,
                        in_string: !in_string,
                    }
                } else {
                    qeuery_sql.push(char);
                    ParserState::SearchingParamStart {
                        escape_next,
                        in_string,
                    }
                }
            }
            ParserState::FoundParam {
                in_string,
                param_name,
            } => {
                if char.is_alphanumeric() {
                    ParserState::FoundParam {
                        in_string,
                        param_name: param_name + &char.to_string(),
                    }
                } else {
                    qeuery_sql.push(char);
                    query_params.push(param_name);
                    if char == ';' {
                        if in_string {
                            ParserState::SearchingParamStart {
                                in_string,
                                escape_next: false,
                            }
                        } else {
                            found_queries.push(try_construct_parsed_sql(
                                query_config,
                                qeuery_sql,
                                query_params,
                            ));
                            query_config = Default::default();
                            qeuery_sql = Default::default();
                            query_params = Default::default();
                            query_param_count = 0;
                            ParserState::Searching
                        }
                    } else if char == '\'' {
                        ParserState::SearchingParamStart {
                            in_string: !in_string,
                            escape_next: false,
                        }
                    } else if char == '\\' {
                        ParserState::SearchingParamStart {
                            in_string,
                            escape_next: true,
                        }
                    } else {
                        ParserState::SearchingParamStart {
                            in_string,
                            escape_next: false,
                        }
                    }
                }
            }
        }
    }
    Ok(found_queries)
}

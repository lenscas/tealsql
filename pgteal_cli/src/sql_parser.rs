use std::{collections::HashMap, error::Error, fmt::Display, fs::read_to_string, path::Path};

use anyhow::Context;

fn show_config(config: &HashMap<String, String>) -> String {
    config
        .iter()
        .map(|(key, value)| format!("{key} = {value}\n"))
        .collect::<String>()
}

pub(crate) struct ParsedSql {
    pub sql: String,
    pub name: String,
    pub params: Vec<String>,
    pub create_execute: bool,
    pub create_fetch_optional: bool,
    pub create_fetch_one: bool,
    pub create_fetch_all: bool,
    pub overwrite_input_name: Option<String>,
    pub overwrite_output_name: Option<String>,
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
#[derive(Debug)]
enum ParseErrors {
    UnexpectedChar {
        expected: Vec<String>,
        got_char: char,
        at_line: u32,
        at_char: u32,
        state: ParserState,
    },
    DuplicateConfigKey {
        duplicate_key: String,
        config: HashMap<String, String>,
        duplicate_at: u32,
    },
    NoNameInConfig {
        config: HashMap<String, String>,
        query: String,
    },
    InvalidConfigValue {
        query: String,
        name: String,
        value: String,
        expected: Vec<String>,
    },
}
impl Error for ParseErrors {}
impl Display for ParseErrors {
    fn fmt(&self, x: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            ParseErrors::UnexpectedChar {
                expected,
                got_char,
                at_line,
                at_char,
                state: _state,
            } => write!(
                x,
                "Expected `{}`. Got {}. At line: {}, character: {}.",
                expected.join("` , `"),
                got_char,
                at_line,
                at_char,
            ),
            ParseErrors::DuplicateConfigKey {
                duplicate_key,
                config,
                duplicate_at,
            } => write!(
                x,
                "Duplicate key `{duplicate_key}`, duplicate at line: {duplicate_at}.\nConfig:\n{}",
                show_config(config)
            ),
            ParseErrors::NoNameInConfig { config, query } => {
                let similar_names = config
                    .iter()
                    .filter(|(key, _)| key.to_lowercase() == "name")
                    .collect::<Vec<_>>();
                if similar_names.is_empty() {
                    write!(
                        x,
                        "No name found in config for query.\nConfig:\n{}\nParsed Query:{}\n",
                        show_config(config),
                        query
                    )
                } else {
                    write!(
                        x,
                        "No name found in config for query.\nConfig:\n{}\nParsed Query:{}\n\nNote: Values found with a similar name:\n{}",
                        show_config(config),
                        query,
                        similar_names.iter().map(|(key,value)| format!("{key} = {value}\n")).collect::<String>()
                    )
                }
            }
            ParseErrors::InvalidConfigValue {
                query,
                value,
                expected,
                name,
            } => write!(
                x,
                "Invalid value for field `{name}` in query: `{query}`. Got `{value}`, expected one of: `{}`",
                expected.join("` , `")
            ),
        }
    }
}

fn create_unexpected_char(
    got_char: char,
    expected: &[&str],
    at_line: u32,
    at_char: u32,
    state: ParserState,
) -> Result<ParserState, ParseErrors> {
    Err(ParseErrors::UnexpectedChar {
        got_char,
        expected: expected.iter().map(|v| v.to_string()).collect(),
        at_line,
        at_char,
        state,
    })
}

fn get_should_create_function(
    config: &HashMap<String, String>,
    field: &str,
    name: &str,
) -> Result<bool, ParseErrors> {
    match config.get(field).map(|v| v.as_str()) {
        Some("true") | None => Ok(true),
        Some("false") => Ok(false),
        Some(x) => Err(ParseErrors::InvalidConfigValue {
            query: name.to_string(),
            name: field.to_string(),
            value: x.to_string(),
            expected: vec!["true".to_string(), "false".to_string()],
        }),
    }
}

fn try_construct_parsed_sql(
    mut config: HashMap<String, String>,
    sql: String,
    params: Vec<String>,
) -> Result<ParsedSql, ParseErrors> {
    let name = match config.remove("name") {
        Some(x) => x,
        None => return Err(ParseErrors::NoNameInConfig { config, query: sql }),
    };
    Ok(ParsedSql {
        sql,
        params,
        create_execute: get_should_create_function(&config, "create_execute", &name)?,
        create_fetch_optional: get_should_create_function(&config, "create_fetch_optional", &name)?,
        create_fetch_one: get_should_create_function(&config, "create_fetch_one", &name)?,
        create_fetch_all: get_should_create_function(&config, "create_fetch_all", &name)?,
        name,
        overwrite_input_name: config.remove("input_name"),
        overwrite_output_name: config.remove("output_name"),
    })
}
pub(crate) fn parse_sql_file(file: &Path) -> Result<Vec<ParsedSql>, anyhow::Error> {
    let res = read_to_string(file)?;
    parse_sql(res).with_context(|| format!("Error in file: {}", file.to_string_lossy()))
}

#[derive(Default)]
struct QueryData {
    config: HashMap<String, String>,
    params: Vec<String>,
    sql: String,
}
impl QueryData {
    fn push_char_to_sql(&mut self, ch: char) {
        self.sql.push(ch)
    }
    fn try_construct_parsed_sql(
        &mut self,
        query_store: &mut Vec<ParsedSql>,
    ) -> Result<(), ParseErrors> {
        let this = std::mem::take(self);
        query_store.push(try_construct_parsed_sql(
            this.config,
            this.sql,
            this.params,
        )?);
        Ok(())
    }
    fn add_param(&mut self, param: String) {
        self.params.push(param);
        self.sql.push('$');
        self.sql.push_str(&self.params.len().to_string());
    }
    fn add_to_config(mut self, name: String, value: String, at: u32) -> Result<Self, ParseErrors> {
        let duplicate = match self.config.entry(name) {
            std::collections::hash_map::Entry::Occupied(x) => Some(x.key().to_owned()),
            std::collections::hash_map::Entry::Vacant(x) => {
                x.insert(value);
                None
            }
        };
        match duplicate {
            None => Ok(self),
            Some(x) => Err(ParseErrors::DuplicateConfigKey {
                config: self.config,
                duplicate_at: at,
                duplicate_key: x,
            }),
        }
    }
}

fn parse_sql(sql_file_contents: String) -> Result<Vec<ParsedSql>, anyhow::Error> {
    let mut state = ParserState::Searching;
    let mut at_line = 1;
    let mut at_char = 0;
    let mut found_queries: Vec<ParsedSql> = Vec::new();

    let mut query_data: QueryData = Default::default();
    for char in sql_file_contents.chars() {
        at_char += 1;
        if char == '\n' {
            at_line += 1;
            at_char = 0;
        }
        let current_state = state.clone();
        let create_error =
            move |expected| create_unexpected_char(char, expected, at_line, at_char, current_state);
        state = match state {
            ParserState::Searching => {
                if char == '/' {
                    ParserState::ParseCommentPart
                } else if char == '*' {
                    ParserState::ParseEndComment
                } else if char.is_whitespace() {
                    ParserState::Searching
                } else {
                    create_error(&["/"])?
                }
            }
            ParserState::ParseCommentPart => {
                if char == '*' {
                    ParserState::InConfigComment
                } else {
                    create_error(&["*"])?
                }
            }
            ParserState::InConfigComment => {
                if char == '@' {
                    ParserState::ParsingName(String::new())
                } else if char == '*' {
                    ParserState::ParseEndComment
                } else {
                    ParserState::InConfigComment
                }
            }
            ParserState::ParsingName(x) => {
                if char.is_alphanumeric() || char == '_' {
                    ParserState::ParsingName(x + &char.to_string())
                } else if char == '=' {
                    ParserState::SearchingValue(x)
                } else if char.is_whitespace() {
                    ParserState::SearchingSeparator(x)
                } else {
                    create_error(&["name", "_", "whitespace", "="])?
                }
            }
            ParserState::SearchingSeparator(x) => {
                if char == '=' {
                    ParserState::SearchingValue(x)
                } else if char.is_whitespace() {
                    ParserState::SearchingSeparator(x)
                } else {
                    create_error(&["=", "whitespace"])?
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
                    create_error(&["alphanumeric", "_", "whitespace"])?
                }
            }
            ParserState::ParsingValue { name, value } => {
                if char.is_alphanumeric() || char == '_' {
                    ParserState::ParsingValue {
                        name,
                        value: value + &char.to_string(),
                    }
                } else if char.is_whitespace() {
                    query_data = query_data.add_to_config(name, value, at_line)?;
                    ParserState::InConfigComment
                } else if char == '*' {
                    ParserState::ParseEndComment
                } else {
                    create_error(&["alphanumeric", "_", "whitespace", "*"])?
                }
            }
            ParserState::ParseEndComment => {
                if char == '/' {
                    ParserState::SearchingParamStart {
                        in_string: false,
                        escape_next: false,
                    }
                } else {
                    ParserState::InConfigComment
                }
            }
            ParserState::SearchingParamStart {
                in_string,
                escape_next,
            } => {
                if escape_next {
                    if char == ':' {
                        query_data.push_char_to_sql(':');
                        //query_sql.push(':');
                    } else if char == '\\' {
                        query_data.push_char_to_sql('\\');
                    } else {
                        query_data.push_char_to_sql('\\');
                        query_data.push_char_to_sql(char);
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
                        query_data.push_char_to_sql(';');
                        ParserState::SearchingParamStart {
                            in_string,
                            escape_next,
                        }
                    } else {
                        query_data.push_char_to_sql(';');
                        query_data.try_construct_parsed_sql(&mut found_queries)?;
                        ParserState::Searching
                    }
                } else if char == ':' {
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
                    query_data.push_char_to_sql(char);
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
                    query_data.add_param(param_name);
                    query_data.push_char_to_sql(char);
                    if char == ';' {
                        if in_string {
                            ParserState::SearchingParamStart {
                                in_string,
                                escape_next: false,
                            }
                        } else {
                            query_data.try_construct_parsed_sql(&mut found_queries)?;
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

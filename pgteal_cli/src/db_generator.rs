use std::{collections::HashMap, fmt::Display};

use anyhow::Context;
use shared::TypeInformation;
use sqlx::{Column, Executor, PgPool, Row, TypeInfo};

use crate::app::HelperForTableConfig;

struct TableRowInformation {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    column_default: Option<String>,
    is_nullable: String,
    data_type: String,
}

#[derive(Debug, Clone)]
enum FailedRowInformationParsing {
    FailedToParseRowType {
        from: String,
        db_name: String,
        schema: String,
        table_name: String,
        column_name: String,
    },
}
impl std::error::Error for FailedRowInformationParsing {}
impl Display for FailedRowInformationParsing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailedRowInformationParsing::FailedToParseRowType {
                from,
                db_name,
                schema,
                table_name,
                column_name,
            } => writeln!(
                f,
                "Could not parse type information for row {schema}.{db_name}.{table_name}.{column_name}.\nFound type {from}"
            ),
        }
    }
}

async fn get_usable_types_for_table(
    table_name: &str,
    row_information: Vec<TableRowInformation>,
    pool: PgPool,
) -> anyhow::Result<Vec<RowInformation>> {
    let columns = row_information
        .iter()
        .map(|v| v.column_name.to_string())
        .collect::<Vec<String>>()
        .join(",");
    let query = format!("SELECT {columns} FROM {table_name}");
    let x = pool.describe(&query).await?;

    let iter = x
        .columns()
        .iter()
        .map(|v| (v.name(), std::slice::from_ref(v.type_info())));
    let mut fields = iter
        .map(|(key, teal_type)| {
            (
                key.to_string(),
                teal_type
                    .iter()
                    .map(|v| v.name())
                    .filter_map(shared::TypeInformation::parse_str)
                    .next(),
            )
        })
        .filter_map(|(key, t)| t.map(|teal_type| (key, teal_type)))
        .collect::<HashMap<String, TypeInformation>>();
    let rows = row_information
        .into_iter()
        .map(|row| {
            let lua_type = fields.remove(&row.column_name).ok_or_else(|| {
                FailedRowInformationParsing::FailedToParseRowType {
                    from: row.data_type.clone(),
                    db_name: row.table_catalog.clone(),
                    schema: row.table_schema.clone(),
                    table_name: row.table_name.clone(),
                    column_name: row.column_name.clone(),
                }
            })?;
            Ok(RowInformation {
                column_name: row.column_name,
                _ordinal_position: row.ordinal_position,
                _column_default: row.column_default,
                _is_nullable: row.is_nullable != "NO",
                data_type: lua_type,
            })
        })
        .collect();
    rows
}

struct RowInformation {
    column_name: String,
    _ordinal_position: i32,
    _column_default: Option<String>,
    _is_nullable: bool,
    data_type: TypeInformation,
}

struct TableInformation {
    _table_catalog: String,
    _table_schema: String,
    table_name: String,
    rows: Vec<RowInformation>,
}

async fn get_table_information(
    db_name: &str,
    table_name: &str,
    table_schema: &str,
    connection: PgPool,
) -> anyhow::Result<TableInformation> {
    let sql = "
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        column_default,
        is_nullable,
        data_type
    FROM information_schema.columns
    WHERE
        table_catalog = $1
    AND 
        table_name= $2
    AND 
        table_schema = $3
    ";
    let query = sqlx::query(sql)
        .bind(db_name)
        .bind(table_name)
        .bind(table_schema);
    let rows: Vec<_> = query
        .fetch_all(&connection)
        .await.with_context(||format!("Failure while fetching data from DB while creating table wrappers.\nFailure while getting data for:{table_name}"))?
        .into_iter()
        .map(|x| TableRowInformation {
            column_name: x.get("column_name"),
            ordinal_position: x.get("ordinal_position"),
            column_default: x.get("column_default"),
            is_nullable: x.get("is_nullable"),
            data_type: x.get("data_type"),
            table_catalog: x.get("table_catalog"),
            table_schema: x.get("table_schema"),
            table_name: x.get("table_name"),
        })
        .collect();
    let rows = get_usable_types_for_table(&table_name, rows, connection.clone()).await?;
    Ok(TableInformation {
        rows: rows
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()
            .context("Failed while parsing type information")?,
        _table_catalog: db_name.to_string(),
        table_name: table_name.to_string(),
        _table_schema: table_schema.to_string(),
    })
}

fn generate_table_helpers(table_name: &str, type_path: &str) -> String {
    format!(
        "
        {{{table_name}= {{
            {table_name} = {type_path},
            insert = function (connection:libpgteal.Connection, value: {type_path}): number
                return connection:insert(\"{table_name}\", value as {{string:boolean | integer | number | {{any : any }}  | string}})
            end,
            update = function(connection:libpgteal.Connection, old_value: {type_path}, new_value:{type_path}):number
                return connection:update(\"{table_name}\", old_value as {{string:boolean | integer | number | {{any : any }}  | string}}, new_value as {{string:boolean | integer | number | {{any : any }}  | string}})
            end,
            upsert = function(connection:libpgteal.Connection, new_values: {type_path}, index:string, to_replace_on_conflict: {type_path}):number
                return connection:upsert(\"{table_name}\", new_values as {{string:boolean | integer | number | {{any : any }}  | string}}, index , to_replace_on_conflict as {{string:boolean | integer | number | {{any : any }}  | string}})
            end,
            delete = function(connection:libpgteal.Connection, old_value: {type_path}):number
                return connection:delete(\"{table_name}\", old_value as {{string:boolean | integer | number | {{any : any }}  | string}})
            end,
            select_all = function(connection:libpgteal.Connection):{{{type_path}}}
                return connection:fetch_all(\"SELECT * FROM {table_name}\") as {{{type_path}}}
            end,
            select_by_all = function(connection:libpgteal.Connection, partial:{type_path}):{{{type_path}}}
                local where_part_checks = {{}}
                local where_part_values = {{}}
                local key = 0
                for k,v in pairs(partial as {{string:string}}) do
                    key = key + 1
                    table.insert(where_part_checks,k ..\"=\".. \"$\"..key)
                    table.insert(where_part_values,v)
                end
                local where_parts = table.concat(where_part_checks,\" AND \")
                return connection:fetch_all(\"SELECT * FROM {table_name} WHERE \"..where_parts, where_part_values) as {{{type_path}}}
            end,
            select_one = function(connection:libpgteal.Connection, partial:{type_path}):{type_path}
                local where_part_checks = {{}}
                local where_part_values = {{}}
                local key = 0
                for k,v in pairs(partial as {{string:string}}) do
                    key = key + 1
                    table.insert(where_part_checks,k ..\"=\".. \"$\"..key)
                    table.insert(where_part_values,v)
                end
                local where_parts = table.concat(where_part_checks,\" AND \")
                return connection:fetch_one(\"SELECT * FROM {table_name} WHERE \"..where_parts..\" LIMIT 1\", where_part_values) as {type_path}
            end
        }}
    }}
    "
    )
}

fn table_info_to_teal(a: TableInformation) -> String {
    let table_name = a.table_name;
    let fields = a
        .rows
        .into_iter()
        .map(|v| {
            let column_name = v.column_name;
            let type_name = v.data_type.as_lua();
            format!("            {column_name} : {type_name}")
        })
        .collect::<Vec<String>>()
        .join("\n");
    format!(
        "
        record {table_name}
{fields}
        end
    
    "
    )
}

async fn create_teal_of_tables(
    db_name: &str,
    table_names: &[String],
    schema: &str,
    connection: PgPool,
) -> anyhow::Result<String> {
    let mut types = Vec::with_capacity(table_names.len());
    let mut table_helpers = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        let table_info =
            get_table_information(db_name, table_name, &schema, connection.clone()).await?;
        let path = {
            let mut x = String::with_capacity(table_name.len() + schema.len() + db_name.len());
            x.push_str(db_name);
            x.push('.');
            x.push_str(schema);
            x.push('.');
            x.push_str(table_name);
            x
        };
        let table_helper = generate_table_helpers(&table_info.table_name, &path);
        let generated_mapping = table_info_to_teal(table_info);
        types.push(generated_mapping);
        table_helpers.push(table_helper);
    }
    let parts = types.join("\n");
    let table_funcs = table_helpers.join(",\n");
    Ok(format!(
        "
local libpgteal = require(\"libpgteal\")
local record {db_name}\n
    record {schema}
        {parts}
    end
end
return {table_funcs}
    "
    ))
}

pub(crate) async fn generate_all_table_helpers(
    connection: PgPool,
    config: Vec<HelperForTableConfig>,
) -> anyhow::Result<()> {
    for helper in config {
        let parts = create_teal_of_tables(
            &helper.db,
            &helper.tables,
            &helper.schema,
            connection.clone(),
        )
        .await?;
        let path = helper.file;
        std::fs::create_dir_all(path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "Could not create directories needed. Path `{}` has no parent",
                path.to_string_lossy()
            )
        })?)
        .with_context(|| {
            format!(
                "While creating the directories for {}",
                path.to_string_lossy()
            )
        })?;
        std::fs::write(&path, parts).with_context(|| {
            let db = helper.db;
            let schema = helper.schema;
            let path = path.to_string_lossy();
            format!(
                "
Failed while writing generated table helpers.
File to write to: {path}
Failed for: {db}.{schema}
            "
            )
        })?;
    }
    Ok(())
}

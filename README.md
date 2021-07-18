# tealsql

Tealsql is split up into 2 parts, tealsql and tealsql-cli

## Tealsql

Tealsql is a wrapper for sqlx, bringing its api to lua and teal. For now, the goal is to get the api fully working with postgresql,
later support for other databases may get added.

sqlx uses Rust's drop trait to properly release connections back to the pool, commit transactions, etc. tealsql emulates this by taking in functions
and doing these thing after the function executed.

## Tealsql-cli

The cli tool is similar to [pgtyped](https://github.com/adelsz/pgtyped) but for teal instead of typescript. This is what it does:

1. Read every .sql file that follow the user supplied pattern
1. Extract ever query inside this file, as well as the name comments
1. Have the database describe the extracted queries, allowing the CLI to know what types the query will return and what types the parameters need to be.
1. Generate teal types that fit with the types of the query, as well as a function that will execute the query. The names are made based on the name comment that belongs to the query
1. Write the generated types and function to a .tl file, using the user provided pattern for the path and filename.

Using this tool ensures that your queries are always up to date with your database, allows the teal compiler to make sure you don't pass the wrong types to your query and also lets it know what types are being returned.

Or in other words: More type safety!

### Why tealsql?

Tealsql is still in development, but already contains some features that (if you ask me) make it a better choice compared to the other popular sql libraries.

|                                            | tealsql                                                     | luasql                          | luadbi                          |
| ------------------------------------------ | ----------------------------------------------------------- | ------------------------------- | ------------------------------- |
| converts sql types to their lua equivalant | yes                                                         | no (returns as string only)     | yes (not checked myself)        |
| uses prepared statements                   | yes                                                         | no                              | yes                             |
| has connection pools                       | yes                                                         | no                              | no                              |
| has build in typedefs for teal             | almost                                                      | only external                   | no                              |
| has proper transaction support             | yes                                                         | configureable autocomit support | configureable autocomit support |
| fetch all                                  | yes                                                         | yes                             | yes                             |
| fetch exactly one                          | yes                                                         | no                              | no                              |
| fetch zero or one                          | yes                                                         | no                              | no                              |
| only execute                               | yes                                                         | no                              | yes                             |
| manual escapement                          | no                                                          | yes                             | yes                             |
| lua5.1 support                             | planned                                                     | yes                             | yes                             |
| lua5.2 support                             | planned                                                     | yes                             | yes                             |
| lua5.3 support                             | yes                                                         | yes                             | yes                             |
| lua5.4 support                             | planned                                                     | yes                             | no                              |
| async api support                          | development [#3](https://github.com/lenscas/tealsql/pull/3) | maybe?                          | maybe?                          |

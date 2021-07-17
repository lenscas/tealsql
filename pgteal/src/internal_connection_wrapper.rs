// use std::ops::{Deref, DerefMut};

use either::Either;
use futures::stream::BoxStream;
use sqlx::{Acquire, Database, Error, Executor, Postgres};

#[derive(Debug)]
pub(crate) enum WrappedConnection {
    PoolConnection(sqlx::pool::PoolConnection<Postgres>),
    Connection(sqlx::postgres::PgConnection),
    //Transaction(sqlx::Transaction<'c, Postgres>),
    //ConnectionRef(&'c mut sqlx::postgres::PgConnection),
}

type FetchManyResult<Res, Row> = Result<Either<Res, Row>, Error>;
type StreamResult<'e, Res, Row> = BoxStream<'e, FetchManyResult<Res, Row>>;

impl<'c> Executor<'c> for &'c mut WrappedConnection {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> StreamResult<
        'e,
        <Self::Database as sqlx::Database>::QueryResult,
        <Self::Database as sqlx::Database>::Row,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.fetch_many(query),
            WrappedConnection::Connection(x) => x.fetch_many(query),
            //WrappedConnection::ConnectionRef(x) => x.fetch_many(query),
            //WrappedConnection::Transaction(x) => x.fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.fetch_optional(query),
            WrappedConnection::Connection(x) => x.fetch_optional(query),
            //WrappedConnection::ConnectionRef(x) => x.fetch_optional(query),
            //WrappedConnection::Transaction(x) => x.fetch_optional(query),
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::database::HasStatement<'q>>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.prepare_with(sql, parameters),
            WrappedConnection::Connection(x) => x.prepare_with(sql, parameters),
            //WrappedConnection::ConnectionRef(x) => x.prepare_with(sql, parameters),
            //WrappedConnection::Transaction(x) => x.prepare_with(sql, parameters),
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.describe(sql),
            WrappedConnection::Connection(x) => x.describe(sql),
            //WrappedConnection::ConnectionRef(x) => x.describe(sql),
            //WrappedConnection::Transaction(x) => x.describe(sql),
        }
    }
}

impl<'a> Acquire<'a> for &'a mut WrappedConnection {
    type Database = Postgres;

    type Connection = &'a mut <Postgres as Database>::Connection;

    fn acquire(self) -> futures::future::BoxFuture<'a, Result<Self::Connection, sqlx::Error>> {
        match self {
            WrappedConnection::PoolConnection(x) => x.acquire(),
            WrappedConnection::Connection(x) => x.acquire(),
            //WrappedConnection::Transaction(x) => x.acquire(),
            //WrappedConnection::ConnectionRef(x) => x.acquire(),
        }
    }

    fn begin(
        self,
    ) -> futures::future::BoxFuture<'a, Result<sqlx::Transaction<'a, Self::Database>, sqlx::Error>>
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.begin(),
            WrappedConnection::Connection(x) => x.begin(),
            //WrappedConnection::Transaction(x) => x.begin(),
            //WrappedConnection::ConnectionRef(x) => x.begin(),
        }
    }
}

use std::ops::{Deref, DerefMut};

use either::Either;
use sqlx::{Acquire, Executor, Postgres};

#[derive(Debug)]
pub(crate) enum WrappedConnection<'c> {
    PoolConnection(sqlx::pool::PoolConnection<Postgres>),
    Connection(sqlx::postgres::PgConnection),
    Transaction(sqlx::Transaction<'c, Postgres>),
    ConnectionRef(&'c mut sqlx::postgres::PgConnection),
}

impl<'c> Deref for WrappedConnection<'c> {
    type Target = sqlx::postgres::PgConnection;

    fn deref(&self) -> &Self::Target {
        match self {
            WrappedConnection::PoolConnection(x) => x.deref(),
            WrappedConnection::Connection(x) => x.deref(),
            WrappedConnection::ConnectionRef(x) => x.deref(),
            WrappedConnection::Transaction(x) => x.deref(),
        }
    }
}

impl<'c> DerefMut for WrappedConnection<'c> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            WrappedConnection::PoolConnection(x) => x.deref_mut(),
            WrappedConnection::Connection(x) => x.deref_mut(),
            WrappedConnection::ConnectionRef(x) => x.deref_mut(),
            WrappedConnection::Transaction(x) => x.deref_mut(),
        }
    }
}

impl<'c> Executor<'c> for &'c mut WrappedConnection<'c> {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            Either<
                <Self::Database as sqlx::Database>::QueryResult,
                <Self::Database as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.fetch_many(query),
            WrappedConnection::Connection(x) | &mut WrappedConnection::ConnectionRef(x) => {
                x.fetch_many(query)
            }
            WrappedConnection::Transaction(x) => x.fetch_many(query),
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
            WrappedConnection::Connection(x) | &mut WrappedConnection::ConnectionRef(x) => {
                x.fetch_optional(query)
            }
            WrappedConnection::Transaction(x) => x.fetch_optional(query),
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
            WrappedConnection::Connection(x) | &mut WrappedConnection::ConnectionRef(x) => {
                x.prepare_with(sql, parameters)
            }
            WrappedConnection::Transaction(x) => x.prepare_with(sql, parameters),
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
            WrappedConnection::Connection(x) | &mut WrappedConnection::ConnectionRef(x) => {
                x.describe(sql)
            }
            WrappedConnection::Transaction(x) => x.describe(sql),
        }
    }
}

impl<'c> Acquire<'c> for WrappedConnection<'c> {
    type Database = Postgres;

    type Connection = WrappedConnection<'c>;

    fn acquire(self) -> futures::future::BoxFuture<'c, Result<WrappedConnection<'c>, sqlx::Error>> {
        Box::pin(std::future::ready(Ok(self)))
    }

    fn begin(
        self,
    ) -> futures::future::BoxFuture<'c, Result<sqlx::Transaction<'c, Self::Database>, sqlx::Error>>
    {
        match self {
            WrappedConnection::PoolConnection(x) => x.begin(),
            WrappedConnection::Connection(x) => x.begin(),
            WrappedConnection::Transaction(x) => x.begin(),
            WrappedConnection::ConnectionRef(x) => x.begin(),
        }
    }
}

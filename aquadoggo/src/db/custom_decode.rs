use std::error::Error;

use serde::{Deserialize, Serialize};
use sqlx::database::{Database, HasValueRef};
use sqlx::decode::Decode;
use sqlx::sqlite::SqliteTypeInfo;
use sqlx::types::Type;
use sqlx::Sqlite;

use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;

#[derive(Debug, Deserialize, Serialize)]
pub struct DoggoAuthor(pub Author);

impl Type<Sqlite> for DoggoAuthor {
    fn type_info() -> SqliteTypeInfo {
        <str as Type<Sqlite>>::type_info()
    }
}

impl std::str::FromStr for DoggoAuthor {
    type Err = sqlx::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let author = Author::new(s).unwrap(); // don't unwrap here irl
        Ok(DoggoAuthor(author))
    }
}

impl<'r, DB: Database> Decode<'r, DB> for DoggoAuthor
where
    &'r str: Decode<'r, DB>,
{
    fn decode(
        value: <DB as HasValueRef<'r>>::ValueRef,
    ) -> Result<DoggoAuthor, Box<dyn Error + 'static + Send + Sync>> {
        let value = <&str as Decode<DB>>::decode(value)?;

        Ok(value.parse()?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DoggoLogId(pub LogId);

impl Type<Sqlite> for DoggoLogId {
    fn type_info() -> SqliteTypeInfo {
        <str as Type<Sqlite>>::type_info()
    }
}

impl std::str::FromStr for DoggoLogId {
    type Err = sqlx::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let log_id = s.parse().unwrap(); // don't unwrap here irl
        Ok(DoggoLogId(log_id))
    }
}

impl<'r, DB: Database> Decode<'r, DB> for DoggoLogId
where
    &'r str: Decode<'r, DB>,
{
    fn decode(
        value: <DB as HasValueRef<'r>>::ValueRef,
    ) -> Result<DoggoLogId, Box<dyn Error + 'static + Send + Sync>> {
        let value = <&str as Decode<DB>>::decode(value)?;

        Ok(value.parse()?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DoggoHash(pub Hash);

impl Type<Sqlite> for DoggoHash {
    fn type_info() -> SqliteTypeInfo {
        <str as Type<Sqlite>>::type_info()
    }
}

impl std::str::FromStr for DoggoHash {
    type Err = sqlx::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash: Hash = s.parse().unwrap(); // don't unwrap here irl
        Ok(DoggoHash(hash))
    }
}

impl<'r, DB: Database> Decode<'r, DB> for DoggoHash
where
    &'r str: Decode<'r, DB>,
{
    fn decode(
        value: <DB as HasValueRef<'r>>::ValueRef,
    ) -> Result<DoggoHash, Box<dyn Error + 'static + Send + Sync>> {
        let value = <&str as Decode<DB>>::decode(value)?;

        Ok(value.parse()?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DoggoSeqNum(pub SeqNum);

impl Type<Sqlite> for DoggoSeqNum {
    fn type_info() -> SqliteTypeInfo {
        <str as Type<Sqlite>>::type_info()
    }
}

impl std::str::FromStr for DoggoSeqNum {
    type Err = sqlx::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let seq_num = s.parse().unwrap(); // don't unwrap here irl
        Ok(DoggoSeqNum(seq_num))
    }
}

impl<'r, DB: Database> Decode<'r, DB> for DoggoSeqNum
where
    &'r str: Decode<'r, DB>,
{
    fn decode(
        value: <DB as HasValueRef<'r>>::ValueRef,
    ) -> Result<DoggoSeqNum, Box<dyn Error + 'static + Send + Sync>> {
        let value = <&str as Decode<DB>>::decode(value)?;

        Ok(value.parse()?)
    }
}

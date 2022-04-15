// SPDX-License-Identifier: AGPL-3.0-or-later
use std::convert::TryFrom;

use async_trait::async_trait;
use sqlx::{query, query_as, query_scalar};

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::SeqNum;
use p2panda_rs::hash::Hash;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors as p2panda_errors;
use p2panda_rs::storage_provider::traits::{
    AsStorageEntry, AsStorageLog, EntryStore, LogStore, StorageProvider,
};
use p2panda_rs::{entry::LogId, identity::Author};

use crate::db::models::{EntryRow, Log};
use crate::db::Pool;
use crate::errors::StorageProviderResult;
use crate::rpc::{EntryArgsRequest, EntryArgsResponse, PublishEntryRequest, PublishEntryResponse};

pub struct SqlStorage {
    pub(crate) pool: Pool,
}

/// Trait which handles all storage actions relating to `Log`s.
#[async_trait]
impl LogStore<Log> for SqlStorage {
    /// Insert a log into storage.
    async fn insert_log(&self, log: Log) -> Result<bool, p2panda_errors::LogStorageError> {
        let rows_affected = query(
            "
            INSERT INTO
                logs (author, log_id, document, schema)
            VALUES
                ($1, $2, $3, $4)
            ",
        )
        .bind(log.author().as_str())
        .bind(log.id().as_u64().to_string())
        .bind(log.document_id().as_str())
        .bind(log.schema_id().as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| p2panda_errors::LogStorageError::Custom(e.to_string()))?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Get a log from storage
    async fn get(
        &self,
        author: &Author,
        document_id: &DocumentId,
    ) -> Result<Option<LogId>, p2panda_errors::LogStorageError> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                log_id
            FROM
                logs
            WHERE
                author = $1
                AND document = $2
            ",
        )
        .bind(author.as_str())
        .bind(document_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| p2panda_errors::LogStorageError::Custom(e.to_string()))?;

        // Wrap u64 inside of `P2PandaLog` instance
        let log_id: Option<LogId> =
            result.map(|str| str.parse().expect("Corrupt u64 integer found in database"));

        Ok(log_id)
    }

    /// Determines the next unused log_id of an author.
    async fn next_log_id(&self, author: &Author) -> Result<LogId, p2panda_errors::LogStorageError> {
        // Get all log ids from this author
        let mut result: Vec<String> = query_scalar(
            "
                    SELECT
                        log_id
                    FROM
                        logs
                    WHERE
                        author = $1
                    ",
        )
        .bind(author.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| p2panda_errors::LogStorageError::Custom(e.to_string()))?;

        // Convert all strings representing u64 integers to `LogId` instances
        let mut log_ids: Vec<LogId> = result
            .iter_mut()
            .map(|str| str.parse().expect("Corrupt u64 integer found in database"))
            .collect();

        // The log id selection below expects log ids in sorted order. We can't easily use SQL
        // for this because log IDs are stored as `VARCHAR`, which doesn't sort numbers correctly.
        // A good solution would not require reading all existing log ids to find the next
        // available one. See this issue: https://github.com/p2panda/aquadoggo/issues/67
        log_ids.sort();

        // Find next unused document log by comparing the sequence of known log ids with an
        // sequence of subsequent log ids until we find a gap.
        let mut next_log_id = LogId::default();

        for log_id in log_ids.iter() {
            // Success! Found unused log id
            if next_log_id != *log_id {
                break;
            }

            // Otherwise, try next possible log id
            next_log_id = next_log_id.next().unwrap();
        }

        Ok(next_log_id)
    }
}

/// Trait which handles all storage actions relating to `Entries`.
#[async_trait]
impl EntryStore<EntryRow> for SqlStorage {
    /// Insert an entry into storage.
    async fn insert_entry(
        &self,
        entry: EntryRow,
    ) -> Result<bool, p2panda_errors::EntryStorageError> {
        println!("{:?}", entry);
        let rows_affected = query(
            "
            INSERT INTO
                entries (
                    author,
                    entry_bytes,
                    entry_hash,
                    log_id,
                    payload_bytes,
                    payload_hash,
                    seq_num
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(entry.author().as_str())
        .bind(entry.entry_signed().as_str())
        .bind(entry.hash().as_str())
        .bind(entry.log_id().as_u64().to_string())
        .bind(entry.operation_encoded().unwrap().as_str())
        .bind(entry.operation_encoded().unwrap().hash().as_str())
        .bind(entry.seq_num().as_u64().to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| p2panda_errors::EntryStorageError::Custom(e.to_string()))?
        .rows_affected();

        Ok(rows_affected == 1)
    }

    /// Returns entry at sequence position within an author's log.
    async fn entry_at_seq_num(
        &self,
        author: &Author,
        log_id: &LogId,
        seq_num: &SeqNum,
    ) -> Result<Option<EntryRow>, p2panda_errors::EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
                AND seq_num = $3
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .bind(seq_num.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| p2panda_errors::EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row)
    }

    /// Returns the latest Bamboo entry of an author's log.
    async fn latest_entry(
        &self,
        author: &Author,
        log_id: &LogId,
    ) -> Result<Option<EntryRow>, p2panda_errors::EntryStorageError> {
        let entry_row = query_as::<_, EntryRow>(
            "
            SELECT
                author,
                entry_bytes,
                entry_hash,
                log_id,
                payload_bytes,
                payload_hash,
                seq_num
            FROM
                entries
            WHERE
                author = $1
                AND log_id = $2
            ORDER BY
                seq_num DESC
            LIMIT
                1
            ",
        )
        .bind(author.as_str())
        .bind(log_id.as_u64().to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| p2panda_errors::EntryStorageError::Custom(e.to_string()))?;

        Ok(entry_row)
    }

    /// Return vector of all entries of a given schema
    async fn by_schema(
        &self,
        schema: &SchemaId,
    ) -> Result<Vec<EntryRow>, p2panda_errors::EntryStorageError> {
        let entries = query_as::<_, EntryRow>(
            "
            SELECT
                entries.author,
                entries.entry_bytes,
                entries.entry_hash,
                entries.log_id,
                entries.payload_bytes,
                entries.payload_hash,
                entries.seq_num
            FROM
                entries
            INNER JOIN logs
                ON (entries.log_id = logs.log_id
                    AND entries.author = logs.author)
            WHERE
                logs.schema = $1
            ",
        )
        .bind(schema.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| p2panda_errors::EntryStorageError::Custom(e.to_string()))?;

        Ok(entries)
    }
}

/// All other methods needed to be implemented by a p2panda `StorageProvider`
#[async_trait]
impl StorageProvider<EntryRow, Log> for SqlStorage {
    type EntryArgsResponse = EntryArgsResponse;
    type EntryArgsRequest = EntryArgsRequest;
    type PublishEntryResponse = PublishEntryResponse;
    type PublishEntryRequest = PublishEntryRequest;

    /// Returns the related document for any entry.
    ///
    /// Every entry is part of a document and, through that, associated with a specific log id used
    /// by this document and author. This method returns that document id by looking up the log
    /// that the entry was stored in.
    async fn get_document_by_entry(
        &self,
        entry_hash: &Hash,
    ) -> StorageProviderResult<Option<DocumentId>> {
        let result: Option<String> = query_scalar(
            "
            SELECT
                logs.document
            FROM
                logs
            INNER JOIN entries
                ON (logs.log_id = entries.log_id
                    AND logs.author = entries.author)
            WHERE
                entries.entry_hash = $1
            ",
        )
        .bind(entry_hash.as_str())
        .fetch_optional(&self.pool)
        .await?;

        // Unwrap here since we already validated the hash
        let hash = result.map(|str| {
            Hash::new(&str)
                .expect("Corrupt hash found in database")
                .into()
        });

        Ok(hash)
    }
}
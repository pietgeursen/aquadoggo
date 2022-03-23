// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::Author;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::errors::LogStorageError;
use p2panda_rs::storage_provider::models::Log as P2PandaLog;
use p2panda_rs::storage_provider::traits::AsStorageLog;
use sqlx::FromRow;

/// Tracks the assigment of an author's logs to documents and records their schema.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
///
/// We store the u64 integer values of `log_id` as a string here since not all database backends
/// support large numbers.
#[derive(FromRow, Debug, Clone)]
pub struct Log {
    /// Public key of the author.
    pub author: String,

    /// Log id used for this document.
    pub log_id: String,

    /// Hash that identifies the document this log is for.
    pub document: String,

    /// SchemaId which identifies the schema for operations in this log.
    pub schema: String,
}

impl AsStorageLog for Log {
    fn new(log: P2PandaLog) -> Self {
        let schema_id = match log.schema().clone() {
            SchemaId::Application(pinned_relation) => {
                let mut id_str = "".to_string();
                let mut relation_iter = pinned_relation.into_iter().peekable();
                while let Some(hash) = relation_iter.next() {
                    id_str += hash.as_str();
                    if relation_iter.peek().is_none() {
                        id_str += "_"
                    }
                }
                id_str
            }
            SchemaId::Schema => "schema_v1".to_string(),
            SchemaId::SchemaField => "schema_field_v1".to_string(),
        };

        Self {
            author: log.author().as_str().to_string(),
            log_id: log.log_id().as_u64().to_string(),
            document: log.document().as_str().to_string(),
            schema: schema_id,
        }
    }

    fn author(&self) -> Author {
        Author::new(&self.author).unwrap()
    }
    fn log_id(&self) -> LogId {
        LogId::from_str(&self.log_id).unwrap()
    }
    fn document(&self) -> DocumentId {
        let document_id: DocumentId = self.document.parse().unwrap();
        document_id
    }
    fn schema(&self) -> SchemaId {
        let schema_id: SchemaId = self.document.parse().unwrap();
        schema_id
    }
}

impl From<P2PandaLog> for Log {
    fn from(log: P2PandaLog) -> Self {
        Log::new(log)
    }
}

impl TryInto<P2PandaLog> for Log {
    type Error = LogStorageError;

    fn try_into(self) -> Result<P2PandaLog, Self::Error> {
        Ok(P2PandaLog {
            author: self.author(),
            log_id: self.log_id(),
            document: self.document(),
            schema: self.schema(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{sign_and_encode, Entry as P2PandaEntry, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};
    use p2panda_rs::schema::SchemaId;
    use p2panda_rs::storage_provider::models::{EntryWithOperation, Log as P2PandaLog};
    use p2panda_rs::storage_provider::traits::{EntryStore, LogStore, StorageProvider};

    use crate::db::models::Entry;
    use crate::db::sql_storage::SqlStorage;
    use crate::test_helpers::{initialize_db, random_entry_hash};

    const TEST_AUTHOR: &str = "58223678ab378f1b07d1d8c789e6da01d16a06b1a4d17cc10119a0109181156c";

    #[tokio::test]
    async fn initial_log_id() {
        let pool = initialize_db().await;

        let author = Author::new(TEST_AUTHOR).unwrap();

        let storage_provider = SqlStorage { pool };

        let log_id = storage_provider
            .find_document_log_id(&author, None)
            .await
            .unwrap();

        assert_eq!(log_id, LogId::new(1));
    }

    #[tokio::test]
    async fn prevent_duplicate_log_ids() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();
        let document = Hash::new(&random_entry_hash()).unwrap();
        let schema = SchemaId::new(&random_entry_hash()).unwrap();

        let log = P2PandaLog::new(&author, &schema, &document.clone().into(), &LogId::new(1));
        assert!(storage_provider.insert_log(log.into()).await.is_ok());

        let log = P2PandaLog::new(&author, &schema, &document.into(), &LogId::new(1));
        assert!(storage_provider.insert_log(log.into()).await.is_err());
    }

    #[async_std::test]
    async fn with_multi_hash_schema_id() {
        let pool = initialize_db().await;
        let storage_provider = SqlStorage { pool };

        let author = Author::new(TEST_AUTHOR).unwrap();
        let document = Hash::new(&random_entry_hash()).unwrap();
        let schema = SchemaId::try_from(DocumentViewId::new(vec![
            Hash::new(&random_entry_hash()).unwrap().into(),
            Hash::new(&random_entry_hash()).unwrap().into(),
        ]))
        .unwrap();

        let log = P2PandaLog::new(&author, &schema, &document.into(), &LogId::new(1));

        assert!(storage_provider.insert_log(log.into()).await.is_ok());
    }

    #[tokio::test]
    async fn selecting_next_log_id() {
        let pool = initialize_db().await;
        let key_pair = KeyPair::new();
        let author = Author::try_from(*key_pair.public_key()).unwrap();
        let schema = SchemaId::new(Hash::new_from_bytes(vec![1, 2, 3]).unwrap().as_str()).unwrap();

        let storage_provider = SqlStorage { pool };

        let log_id = storage_provider
            .find_document_log_id(&author, None)
            .await
            .unwrap();

        // We expect to be given the next log id when asking for a possible log id for a new
        // document by the same author
        assert_eq!(log_id, LogId::default());

        // Starting with an empty db, we expect to be able to count up from 1 and expect each
        // inserted document's log id to be euqal to the count index
        for n in 1..12 {
            let doc = Hash::new_from_bytes(vec![1, 2, n]).unwrap().into();

            let log_id = storage_provider
                .find_document_log_id(&author, None)
                .await
                .unwrap();
            assert_eq!(LogId::new(n.into()), log_id);
            let log = P2PandaLog::new(&author, &schema, &doc, &log_id);
            storage_provider.insert_log(log.into()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn document_log_id() {
        let pool = initialize_db().await;

        // Create a new document
        // TODO: use p2panda-rs test utils once available
        let key_pair = KeyPair::new();
        let author = Author::try_from(*key_pair.public_key()).unwrap();
        let log_id = LogId::new(1);
        let schema = SchemaId::new(Hash::new_from_bytes(vec![1, 2, 3]).unwrap().as_str()).unwrap();
        let seq_num = SeqNum::new(1).unwrap();
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = Operation::new_create(schema.clone(), fields).unwrap();
        let operation_encoded = OperationEncoded::try_from(&operation).unwrap();
        let entry = P2PandaEntry::new(&log_id, Some(&operation), None, None, &seq_num).unwrap();
        let entry_encoded = sign_and_encode(&entry, &key_pair).unwrap();

        let storage_provider = SqlStorage { pool };

        // Expect database to return nothing yet
        assert_eq!(
            storage_provider
                .get_document_by_entry(&entry_encoded.hash())
                .await
                .unwrap(),
            None
        );

        let entry_with_operation =
            EntryWithOperation::new(&entry_encoded.clone(), &operation_encoded).unwrap();

        let entry = Entry::try_from(entry_with_operation).unwrap();

        // Store entry in database
        assert!(storage_provider.insert_entry(entry).await.is_ok());

        let log = P2PandaLog::new(
            &author,
            &schema,
            &entry_encoded.hash().into(),
            &LogId::new(1),
        );

        // Store log in database
        assert!(storage_provider.insert_log(log.into()).await.is_ok());

        // Expect to find document in database. The document hash should be the same as the hash of
        // the entry which referred to the `CREATE` operation.
        assert_eq!(
            storage_provider
                .get_document_by_entry(&entry_encoded.hash())
                .await
                .unwrap(),
            Some(entry_encoded.hash().into())
        );

        // We expect to find this document in the default log
        assert_eq!(
            storage_provider
                .find_document_log_id(&author, Some(&entry_encoded.hash().into()))
                .await
                .unwrap(),
            LogId::default()
        );
    }

    #[tokio::test]
    async fn log_ids() {
        let pool = initialize_db().await;

        // Mock author
        let author = Author::new(TEST_AUTHOR).unwrap();

        // Mock schema
        let schema = SchemaId::new(&random_entry_hash()).unwrap();

        // Mock four different document hashes
        let document_first = Hash::new(&random_entry_hash()).unwrap();
        let document_second = Hash::new(&random_entry_hash()).unwrap();
        let document_third = Hash::new(&random_entry_hash()).unwrap();
        let document_forth = Hash::new(&random_entry_hash()).unwrap();

        let storage_provider = SqlStorage { pool };

        // Register two log ids at the beginning
        let log_1 = P2PandaLog::new(&author, &schema, &document_first.into(), &LogId::new(1));
        let log_2 = P2PandaLog::new(&author, &schema, &document_second.into(), &LogId::new(2));

        storage_provider.insert_log(log_1.into()).await.unwrap();
        storage_provider.insert_log(log_2.into()).await.unwrap();

        // Find next free log id and register it
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(3));

        let log_3 = P2PandaLog::new(&author, &schema, &document_third.into(), &log_id);

        storage_provider.insert_log(log_3.into()).await.unwrap();

        // Find next free log id and register it
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(4));

        let log_4 = P2PandaLog::new(&author, &schema, &document_forth.into(), &log_id);

        storage_provider.insert_log(log_4.into()).await.unwrap();

        // Find next free log id
        let log_id = storage_provider.next_log_id(&author).await.unwrap();
        assert_eq!(log_id, LogId::new(5));
    }
}

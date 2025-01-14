// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::entry::decode_entry;
use p2panda_rs::operation::{AsOperation, Operation};
use p2panda_rs::Validate;

use crate::db::models::{Entry, Log};
use crate::errors::Result;
use crate::rpc::request::PublishEntryRequest;
use crate::rpc::response::PublishEntryResponse;
use crate::rpc::RpcApiState;

#[derive(thiserror::Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum PublishEntryError {
    #[error("Could not find backlink entry in database")]
    BacklinkMissing,

    #[error("Could not find skiplink entry in database")]
    SkiplinkMissing,

    #[error("Could not find document hash for entry in database")]
    DocumentMissing,

    #[error("UPDATE or DELETE operation came with an entry without backlink")]
    OperationWithoutBacklink,

    #[error("Requested log id {0} does not match expected log id {1}")]
    InvalidLogId(u64, u64),
}

/// Implementation of `panda_publishEntry` RPC method.
///
/// Stores an author's Bamboo entry with operation payload in database after validating it.
pub async fn publish_entry(
    data: Data<RpcApiState>,
    Params(params): Params<PublishEntryRequest>,
) -> Result<PublishEntryResponse> {
    // Validate request parameters
    params.entry_encoded.validate()?;
    params.operation_encoded.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Decode author, entry and operation. This conversion validates the operation hash
    let author = params.entry_encoded.author();
    let entry = decode_entry(&params.entry_encoded, Some(&params.operation_encoded))?;
    let operation = Operation::from(&params.operation_encoded);

    // Every operation refers to a document we need to determine. A document is identified by the
    // hash of its first `CREATE` operation, it is the root operation of every document graph
    let document_id = if operation.is_create() {
        // This is easy: We just use the entry hash directly to determine the document id
        params.entry_encoded.hash()
    } else {
        // For any other operations which followed after creation we need to either walk the operation
        // graph back to its `CREATE` operation or more easily look up the database since we keep track
        // of all log ids and documents there.
        //
        // We can determine the used document hash by looking at what we know about the previous
        // entry in this author's log.
        //
        // @TODO: This currently looks at the backlink, in the future we want to use
        // "previousOperation", since in a multi-writer setting there might be no backlink for
        // update operations! See: https://github.com/p2panda/aquadoggo/issues/49
        let backlink_entry_hash = entry
            .backlink_hash()
            .ok_or(PublishEntryError::OperationWithoutBacklink)?;

        Log::get_document_by_entry(&pool, backlink_entry_hash)
            .await?
            .ok_or(PublishEntryError::DocumentMissing)?
    };

    // Determine expected log id for new entry
    let document_log_id = Log::find_document_log_id(&pool, &author, Some(&document_id)).await?;

    // Check if provided log id matches expected log id
    if &document_log_id != entry.log_id() {
        return Err(PublishEntryError::InvalidLogId(
            entry.log_id().as_u64(),
            document_log_id.as_u64(),
        )
        .into());
    }

    // Get related bamboo backlink and skiplink entries
    let entry_backlink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            entry.log_id(),
            &entry.seq_num_backlink().unwrap(),
        )
        .await?
        .map(|link| {
            let bytes = hex::decode(link.entry_bytes)
                .expect("Backlink entry with invalid hex-encoding detected in database");
            Some(bytes)
        })
        .ok_or(PublishEntryError::BacklinkMissing)
    } else {
        Ok(None)
    }?;

    let entry_skiplink_bytes = if !entry.seq_num().is_first() {
        Entry::at_seq_num(
            &pool,
            &author,
            entry.log_id(),
            &entry.seq_num_skiplink().unwrap(),
        )
        .await?
        .map(|link| {
            let bytes = hex::decode(link.entry_bytes)
                .expect("Backlink entry with invalid hex-encoding detected in database");
            Some(bytes)
        })
        .ok_or(PublishEntryError::SkiplinkMissing)
    } else {
        Ok(None)
    }?;

    // Verify bamboo entry integrity, including encoding, signature of the entry correct back- and
    // skiplinks.
    bamboo_rs_core_ed25519_yasmf::verify(
        &params.entry_encoded.to_bytes(),
        Some(&params.operation_encoded.to_bytes()),
        entry_skiplink_bytes.as_deref(),
        entry_backlink_bytes.as_deref(),
    )?;

    // Register log in database when a new document is created
    if operation.is_create() {
        Log::insert(
            &pool,
            &author,
            &document_id,
            &operation.schema(),
            entry.log_id(),
        )
        .await?;
    }

    // Finally insert Entry in database
    Entry::insert(
        &pool,
        &author,
        &params.entry_encoded,
        &params.entry_encoded.hash(),
        entry.log_id(),
        &params.operation_encoded,
        &params.operation_encoded.hash(),
        entry.seq_num(),
    )
    .await?;

    // Already return arguments for next entry creation
    let mut entry_latest = Entry::latest(&pool, &author, entry.log_id())
        .await?
        .expect("Database does not contain any entries");
    let entry_hash_skiplink = super::entry_args::determine_skiplink(pool, &entry_latest).await?;
    let next_seq_num = entry_latest.seq_num.next().unwrap();

    Ok(PublishEntryResponse {
        entry_hash_backlink: Some(params.entry_encoded.hash()),
        entry_hash_skiplink,
        seq_num: next_seq_num.as_u64().to_string(),
        log_id: entry.log_id().as_u64().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use p2panda_rs::entry::{sign_and_encode, Entry, EntrySigned, LogId, SeqNum};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{Operation, OperationEncoded, OperationFields, OperationValue};

    use crate::server::{build_server, ApiState};
    use crate::test_helpers::{
        handle_http, initialize_db, rpc_error, rpc_request, rpc_response, TestClient,
    };

    /// Create encoded entries and operations for testing.
    fn create_test_entry(
        key_pair: &KeyPair,
        schema: &Hash,
        log_id: &LogId,
        document: Option<&Hash>,
        skiplink: Option<&EntrySigned>,
        backlink: Option<&EntrySigned>,
        seq_num: &SeqNum,
    ) -> (EntrySigned, OperationEncoded) {
        // Create operation with dummy data
        let mut fields = OperationFields::new();
        fields
            .add("test", OperationValue::Text("Hello".to_owned()))
            .unwrap();
        let operation = match document {
            Some(_) => {
                Operation::new_update(schema.clone(), vec![backlink.unwrap().hash()], fields)
                    .unwrap()
            }
            None => Operation::new_create(schema.clone(), fields).unwrap(),
        };

        // Encode operation
        let operation_encoded = OperationEncoded::try_from(&operation).unwrap();

        // Create, sign and encode entry
        let entry = Entry::new(
            log_id,
            Some(&operation),
            skiplink.map(|e| e.hash()).as_ref(),
            backlink.map(|e| e.hash()).as_ref(),
            seq_num,
        )
        .unwrap();
        let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();

        (entry_encoded, operation_encoded)
    }

    /// Compare API response from publishing an encoded entry and operation to expected skiplink,
    /// log id and sequence number.
    async fn assert_request(
        client: &TestClient,
        entry_encoded: &EntrySigned,
        operation_encoded: &OperationEncoded,
        expect_skiplink: Option<&EntrySigned>,
        expect_log_id: &LogId,
        expect_seq_num: &SeqNum,
    ) {
        // Prepare request to API
        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_encoded.as_str(),
                operation_encoded.as_str(),
            ),
        );

        // Prepare expected response result
        let skiplink_str = match expect_skiplink {
            Some(entry) => {
                format!("\"{}\"", entry.hash().as_str())
            }
            None => "null".to_owned(),
        };

        let response = rpc_response(&format!(
            r#"{{
                "entryHashBacklink": "{}",
                "entryHashSkiplink": {},
                "seqNum": "{}",
                "logId": "{}"
            }}"#,
            entry_encoded.hash().as_str(),
            skiplink_str,
            expect_seq_num.as_u64(),
            expect_log_id.as_u64(),
        ));

        assert_eq!(handle_http(&client, request).await, response);
    }

    #[tokio::test]
    async fn publish_entry() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);
        let client = TestClient::new(app);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::default();
        let seq_num_1 = SeqNum::new(1).unwrap();

        // Create a couple of entries in the same log and check for consistency. The little diagrams
        // show back links and skip links analogous to this diagram from the bamboo spec:
        // https://github.com/AljoschaMeyer/bamboo#links-and-entry-verification
        //
        // [1] --
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num_1);
        assert_request(
            &client,
            &entry_1,
            &operation_1,
            None,
            &log_id,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        // [1] <-- [2]
        let (entry_2, operation_2) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );
        assert_request(
            &client,
            &entry_2,
            &operation_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // [1] <-- [2] <-- [3]
        let (entry_3, operation_3) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(3).unwrap(),
        );
        assert_request(
            &client,
            &entry_3,
            &operation_3,
            Some(&entry_1),
            &log_id,
            &SeqNum::new(4).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]
        let (entry_4, operation_4) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            Some(&entry_1),
            Some(&entry_3),
            &SeqNum::new(4).unwrap(),
        );
        assert_request(
            &client,
            &entry_4,
            &operation_4,
            None,
            &log_id,
            &SeqNum::new(5).unwrap(),
        )
        .await;

        //  /------------------ [4]
        // [1] <-- [2] <-- [3]   \-- [5] --
        let (entry_5, operation_5) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_4),
            &SeqNum::new(5).unwrap(),
        );
        assert_request(
            &client,
            &entry_5,
            &operation_5,
            None,
            &log_id,
            &SeqNum::new(6).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn validate() {
        // Create key pair for author
        let key_pair = KeyPair::new();

        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);
        let client = TestClient::new(app);

        // Define schema and log id for entries
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let log_id = LogId::new(1);
        let seq_num = SeqNum::new(1).unwrap();

        // Create two valid entries for testing
        let (entry_1, operation_1) =
            create_test_entry(&key_pair, &schema, &log_id, None, None, None, &seq_num);
        assert_request(
            &client,
            &entry_1,
            &operation_1,
            None,
            &log_id,
            &SeqNum::new(2).unwrap(),
        )
        .await;

        let (entry_2, operation_2) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );
        assert_request(
            &client,
            &entry_2,
            &operation_2,
            None,
            &log_id,
            &SeqNum::new(3).unwrap(),
        )
        .await;

        // Send invalid log id for a new document: The entries entry_1 and entry_2 are assigned to
        // log 1, which makes log 2 the required log for the next new document.
        let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(3),
            None,
            None,
            None,
            &SeqNum::new(1).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                operation_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 3 does not match expected log id 2");
        assert_eq!(handle_http(&client, request).await, response);

        // Send invalid log id for an existing document: This entry is an update for the existing
        // document in log 1, however, we are trying to publish it in log 3.
        let (entry_wrong_log_id, operation_wrong_log_id) = create_test_entry(
            &key_pair,
            &schema,
            &LogId::new(3),
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(2).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_log_id.as_str(),
                operation_wrong_log_id.as_str(),
            ),
        );

        let response = rpc_error("Requested log id 3 does not match expected log id 1");
        assert_eq!(handle_http(&client, request).await, response);

        // Send invalid backlink entry / hash
        let (entry_wrong_hash, operation_wrong_hash) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_1),
            &SeqNum::new(3).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_hash.as_str(),
                operation_wrong_hash.as_str(),
            ),
        );

        let response = rpc_error(
            "The backlink hash encoded in the entry does not match the lipmaa entry provided",
        );
        assert_eq!(handle_http(&client, request).await, response);

        // Send invalid sequence number
        let (entry_wrong_seq_num, operation_wrong_seq_num) = create_test_entry(
            &key_pair,
            &schema,
            &log_id,
            Some(&entry_1.hash()),
            None,
            Some(&entry_2),
            &SeqNum::new(5).unwrap(),
        );

        let request = rpc_request(
            "panda_publishEntry",
            &format!(
                r#"{{
                    "entryEncoded": "{}",
                    "operationEncoded": "{}"
                }}"#,
                entry_wrong_seq_num.as_str(),
                operation_wrong_seq_num.as_str(),
            ),
        );

        let response = rpc_error("Could not find backlink entry in database");
        assert_eq!(handle_http(&client, request).await, response);
    }
}

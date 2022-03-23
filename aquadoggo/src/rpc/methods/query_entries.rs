// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::EntryStore;
use p2panda_rs::Validate;

use crate::db::sql_storage::SqlStorage;
use crate::errors::StorageProviderResult;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QueryEntriesResponse;

pub async fn query_entries(
    storage_provider: Data<SqlStorage>,
    Params(params): Params<QueryEntriesRequest>,
) -> StorageProviderResult<QueryEntriesResponse> {
    // Validate request parameters
    params.schema.validate()?;

    // Convert the hash into a schema id, we want to actually pass schema id here,
    // see comment in rc/request.rs
    let schema_id = SchemaId::new(params.schema.as_str()).unwrap();

    // Find and return raw entries from database
    let entries = storage_provider.by_schema(&schema_id).await?;

    Ok(QueryEntriesResponse { entries })
}

#[cfg(test)]
mod tests {
    use p2panda_rs::hash::Hash;

    use crate::server::{build_server, ApiState};
    use crate::test_helpers::{handle_http, initialize_db, rpc_request, rpc_response, TestClient};

    #[tokio::test]
    async fn query_entries() {
        // Prepare test database
        let pool = initialize_db().await;

        // Create tide server with endpoints
        let state = ApiState::new(pool.clone());
        let app = build_server(state);
        let client = TestClient::new(app);

        // Prepare request to API
        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();
        let request = rpc_request(
            "panda_queryEntries",
            &format!(
                r#"{{
                    "schema": "{}"
                }}"#,
                schema.as_str(),
            ),
        );

        println!("{}", request);

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        assert_eq!(handle_http(&client, request).await, response);
    }
}

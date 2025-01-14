// SPDX-License-Identifier: AGPL-3.0-or-later

use jsonrpc_v2::{Data, Params};
use p2panda_rs::Validate;

use crate::db::models::Entry;
use crate::errors::Result;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QueryEntriesResponse;
use crate::rpc::RpcApiState;

pub async fn query_entries(
    data: Data<RpcApiState>,
    Params(params): Params<QueryEntriesRequest>,
) -> Result<QueryEntriesResponse> {
    // Validate request parameters
    params.schema.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Find and return raw entries from database
    let entries = Entry::by_schema(&pool, &params.schema).await?;
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

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        assert_eq!(handle_http(&client, request).await, response);
    }
}

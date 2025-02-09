// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::net::{SocketAddr, TcpListener};

use axum::body::HttpBody;
use axum::BoxError;
use http::header::{HeaderName, HeaderValue};
use http::{Request, StatusCode};
use hyper::{Body, Server};
use p2panda_rs::hash::Hash;
use rand::Rng;
use sqlx::any::Any;
use sqlx::migrate::MigrateDatabase;
use tower::make::Shared;
use tower_service::Service;

use crate::db::{connection_pool, create_database, run_pending_migrations, Pool};

const DB_URL: &str = "sqlite::memory:";

pub(crate) struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    pub(crate) fn new<S, ResBody>(service: S) -> Self
    where
        S: Service<Request<Body>, Response = http::Response<ResBody>> + Clone + Send + 'static,
        ResBody: HttpBody + Send + 'static,
        ResBody::Data: Send,
        ResBody::Error: Into<BoxError>,
        S::Future: Send,
        S::Error: Into<BoxError>,
    {
        // Setting the port to zero asks the operating system to find one for us
        let listener = TcpListener::bind("127.0.0.1:0").expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let server = Server::from_tcp(listener)
                .unwrap()
                .serve(Shared::new(service));
            server.await.expect("server error");
        });

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        TestClient { client, addr }
    }

    pub(crate) fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.get(format!("http://{}{}", self.addr, url)),
        }
    }

    pub(crate) fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.post(format!("http://{}{}", self.addr, url)),
        }
    }
}

pub(crate) struct RequestBuilder {
    builder: reqwest::RequestBuilder,
}

impl RequestBuilder {
    pub(crate) async fn send(self) -> TestResponse {
        TestResponse {
            response: self.builder.send().await.unwrap(),
        }
    }

    pub(crate) fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn json<T>(mut self, json: &T) -> Self
    where
        T: serde::Serialize,
    {
        self.builder = self.builder.json(json);
        self
    }

    pub(crate) fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        self.builder = self.builder.header(key, value);
        self
    }
}

pub(crate) struct TestResponse {
    response: reqwest::Response,
}

impl TestResponse {
    pub(crate) async fn text(self) -> String {
        self.response.text().await.unwrap()
    }

    #[allow(dead_code)]
    pub(crate) async fn json<T>(self) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        self.response.json().await.unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn status(&self) -> StatusCode {
        self.response.status()
    }
}

// Create test database
pub async fn initialize_db() -> Pool {
    // Reset database first
    drop_database().await;
    create_database(DB_URL).await.unwrap();

    // Create connection pool and run all migrations
    let pool = connection_pool(DB_URL, 5).await.unwrap();
    run_pending_migrations(&pool).await.unwrap();

    pool
}

// Delete test database
pub async fn drop_database() {
    if Any::database_exists(DB_URL).await.unwrap() {
        Any::drop_database(DB_URL).await.unwrap();
    }
}

// Generate random entry hash
pub fn random_entry_hash() -> String {
    let random_data = rand::thread_rng().gen::<[u8; 32]>().to_vec();

    Hash::new_from_bytes(random_data)
        .unwrap()
        .as_str()
        .to_owned()
}

// Helper method to generate valid JSON RPC request string
pub(crate) fn rpc_request(method: &str, params: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "method": "{}",
            "params": {},
            "id": 1
        }}"#,
        method, params
    )
    .replace(" ", "")
    .replace("\n", "")
}

// Helper method to generate valid JSON RPC response string
pub(crate) fn rpc_response(result: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "result": {},
            "id": 1
        }}"#,
        result
    )
    .replace(" ", "")
    .replace("\n", "")
}

// Helper method to generate valid JSON RPC error response string
pub(crate) fn rpc_error(message: &str) -> String {
    format!(
        r#"{{
            "jsonrpc": "2.0",
            "error": {{
                "code": 0,
                "message": "<message>"
            }},
            "id": 1
        }}"#
    )
    .replace(" ", "")
    .replace("\n", "")
    .replace("<message>", message)
}

// Helper method to handle JSON RPC HTTP request and return response
pub(crate) async fn handle_http(client: &TestClient, request: String) -> String {
    let response = client
        .post("/")
        .body(request)
        .header("content-type", HeaderValue::from_static("application/json"))
        .send()
        .await;
    response.text().await
}

//! Integration tests for client operations.

#[allow(dead_code)]
mod common;

use std::net::SocketAddr;
use strata::client::{DataClient, MetadataClient};

#[test]
fn test_metadata_client_creation() {
    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let _client = MetadataClient::new(addr);
    // Client creation should succeed even without a server
}

#[test]
fn test_data_client_creation() {
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let _client = DataClient::new(addr);
    // Client creation should succeed even without a server
}

#[tokio::test]
async fn test_metadata_client_health_no_server() {
    let addr: SocketAddr = "127.0.0.1:19000".parse().unwrap();
    let client = MetadataClient::new(addr);
    // Health check should fail with no server
    let result = client.health().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_data_client_health_no_server() {
    let addr: SocketAddr = "127.0.0.1:19001".parse().unwrap();
    let client = DataClient::new(addr);
    // Health check should fail with no server
    let result = client.health().await;
    assert!(result.is_err());
}

#[test]
fn test_metadata_client_from_addr() {
    let client = MetadataClient::from_addr("127.0.0.1:9000");
    assert!(client.is_ok());
}

#[test]
fn test_metadata_client_from_invalid_addr() {
    let client = MetadataClient::from_addr("not-a-valid-address");
    assert!(client.is_err());
}

#[test]
fn test_data_client_from_addr() {
    let client = DataClient::from_addr("127.0.0.1:9001");
    assert!(client.is_ok());
}

#[test]
fn test_data_client_from_invalid_addr() {
    let client = DataClient::from_addr("invalid:address:format");
    assert!(client.is_err());
}

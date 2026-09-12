use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use trakkin_provider_plex::{adapter::PlexAdapter, mapping};
use trakkin_provider_sdk::{
    v1::{
        ConfigurationValue, DiscoverSourcesRequest, OpenConnectionRequest, ReadCatalogRequest,
        ReadMode, SecretValue, SourceMembership, SubjectReference, TargetedStateWriteIntent,
        TargetedStateWriteStatus, Value, WriteTargetedStateRequest,
        adapter_service_server::AdapterService, open_connection_response, read_catalog_response,
        subject_reference, targeted_state_write_intent, value,
    },
    validation::CatalogStreamValidator,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

const TOKEN: &str = "plex-test-token";

fn configuration(key: &str, text: &str) -> ConfigurationValue {
    ConfigurationValue {
        key: key.to_owned(),
        value: Some(Value {
            value: Some(value::Value::Text(text.to_owned())),
        }),
    }
}

async fn open_adapter(server: &MockServer) -> PlexAdapter {
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-plex-token", TOKEN))
        .and(header("x-plex-client-identifier", "trakkin-test-process"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "machineIdentifier": "server-1",
                "friendlyName": "Home Plex",
                "version": "1.43.2",
                "updatedAt": 100
            }
        })))
        .expect(1)
        .mount(server)
        .await;

    let adapter = PlexAdapter::new("test-process", CancellationToken::new());
    let response = adapter
        .open_connection(Request::new(OpenConnectionRequest {
            settings: vec![configuration("server_url", &server.uri())],
            secrets: vec![SecretValue {
                key: "token".to_owned(),
                value: TOKEN.as_bytes().to_vec(),
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(open_connection_response::Outcome::Result(result)) = response.outcome else {
        panic!("Plex connection did not open");
    };
    assert_eq!(result.accounts[0].display_name, "Home Plex");
    adapter
}

async fn mount_movie_section(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/library/sections/all"))
        .and(header("x-plex-token", TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Directory": [{
                    "key": "1",
                    "uuid": "movies-1",
                    "title": "Movies",
                    "type": "movie",
                    "updatedAt": 200
                }]
            }
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn source_refresh_evicts_removed_libraries() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;
    adapter
        .discover_sources(Request::new(DiscoverSourcesRequest {}))
        .await
        .unwrap();

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/library/sections/all"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": { "Directory": [] }
        })))
        .expect(2)
        .mount(&server)
        .await;
    adapter
        .discover_sources(Request::new(DiscoverSourcesRequest {}))
        .await
        .unwrap();

    let error = match adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"removed-library".to_vec(),
            source_key: Some(mapping::source_key("1")),
            mode: ReadMode::Full as i32,
            prior_cursor: Vec::new(),
            preferred_batch_size: 10,
        }))
        .await
    {
        Ok(_) => panic!("removed Plex library remained readable"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::NotFound);
    server.verify().await;
}

#[tokio::test]
async fn streams_an_authenticated_paginated_catalog() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;

    Mock::given(method("GET"))
        .and(path("/library/sections/1/all"))
        .and(query_param("includeGuids", "1"))
        .and(header("x-plex-container-start", "0"))
        .and(header("x-plex-container-size", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "size": 2,
                "offset": 0,
                "totalSize": 3,
                "Metadata": [
                    {
                        "ratingKey": "10",
                        "key": "/library/metadata/10",
                        "guid": "plex://movie/first",
                        "type": "movie",
                        "title": "First",
                        "year": 2024,
                        "Guid": [{ "id": "imdb://tt0000010" }]
                    },
                    {
                        "ratingKey": "20",
                        "key": "/library/metadata/20",
                        "guid": "plex://movie/second",
                        "type": "movie",
                        "title": "Second"
                    }
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/1/all"))
        .and(query_param("includeGuids", "1"))
        .and(header("x-plex-container-start", "2"))
        .and(header("x-plex-container-size", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "size": 1,
                "offset": 2,
                "totalSize": 3,
                "Metadata": [{
                    "ratingKey": "30",
                    "key": "/library/metadata/30",
                    "guid": "plex://movie/third",
                    "type": "movie",
                    "title": "Third"
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut stream = adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"catalog-page-test".to_vec(),
            source_key: Some(mapping::source_key("1")),
            mode: ReadMode::Full as i32,
            prior_cursor: Vec::new(),
            preferred_batch_size: 2,
        }))
        .await
        .unwrap()
        .into_inner();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }

    let mut validator = CatalogStreamValidator::default();
    for event in &events {
        validator.accept(event).unwrap();
    }
    validator.finish().unwrap();
    assert_eq!(events.len(), 3);
    let Some(read_catalog_response::Event::Batch(first_batch)) = &events[0].event else {
        panic!("first catalog event was not a batch");
    };
    assert_eq!(first_batch.item_upserts.len(), 2);
    assert_eq!(first_batch.item_upserts[0].display_name, "First");
    assert_eq!(first_batch.item_upserts[0].portable_references.len(), 2);
    let Some(read_catalog_response::Event::Batch(second_batch)) = &events[1].event else {
        panic!("second catalog event was not a batch");
    };
    assert_eq!(second_batch.sequence, 1);
    assert_eq!(second_batch.item_upserts[0].display_name, "Third");
    assert!(matches!(
        events[2].event,
        Some(read_catalog_response::Event::Completed(_))
    ));
    server.verify().await;
}

#[tokio::test]
async fn writes_supported_watch_state_to_plex() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;

    Mock::given(method("PUT"))
        .and(path("/:/scrobble"))
        .and(query_param("identifier", "com.plexapp.plugins.library"))
        .and(query_param("key", "42"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/:/timeline"))
        .and(query_param("ratingKey", "42"))
        .and(query_param("key", "/library/metadata/42"))
        .and(query_param("state", "stopped"))
        .and(query_param("time", "1250"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/:/rate"))
        .and(query_param("identifier", "com.plexapp.plugins.library"))
        .and(query_param("key", "42"))
        .and(query_param("rating", "8.5"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let response = adapter
        .write_targeted_state(Request::new(WriteTargetedStateRequest {
            operation_id: b"write-state-test".to_vec(),
            source_key: Some(mapping::source_key("1")),
            subject: Some(SubjectReference {
                subject: Some(subject_reference::Subject::ProviderItemKey(
                    mapping::item_key("42"),
                )),
            }),
            idempotency_key: b"write-state-idempotency".to_vec(),
            expected_membership: SourceMembership::Present as i32,
            precondition: Vec::new(),
            allow_create_membership: false,
            intents: vec![
                TargetedStateWriteIntent {
                    field: Some(mapping::watched_field()),
                    operation: Some(targeted_state_write_intent::Operation::Set(
                        mapping::boolean_value(true),
                    )),
                },
                TargetedStateWriteIntent {
                    field: Some(mapping::progress_field()),
                    operation: Some(targeted_state_write_intent::Operation::Set(
                        mapping::integer_value(1250),
                    )),
                },
                TargetedStateWriteIntent {
                    field: Some(mapping::rating_field()),
                    operation: Some(targeted_state_write_intent::Operation::Set(
                        mapping::decimal_value(8.5),
                    )),
                },
            ],
            maximum_receipt_bytes: 32,
            maximum_response_bytes: 64 * 1024,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, TargetedStateWriteStatus::Applied as i32);
    assert_eq!(response.field_effects.len(), 3);
    assert_eq!(response.receipt.len(), 32);
    assert!(response.error.is_none());
    server.verify().await;
}

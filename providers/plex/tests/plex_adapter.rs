use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};
use trakkin_provider_plex::{adapter::PlexAdapter, mapping};
use trakkin_provider_sdk::{
    v1::{
        AuthenticationStatus, ConfigurationValue, ContinueAuthenticationRequest,
        DescribeConnectionRequest, DiscoverSourcesRequest, LookupPortableReferencesRequest,
        OpenConnectionRequest, OperationFailureCategory, PortableReference, ReadCatalogRequest,
        ReadMode, ReadStateRequest, SecretValue, SourceMembership, StartAuthenticationRequest,
        SubjectReference, TargetedStateWriteIntent, TargetedStateWriteStatus,
        ValidateConnectionRequest, Value, WriteTargetedStateRequest,
        adapter_service_server::AdapterService, continue_authentication_response,
        describe_connection_response, discover_sources_response,
        lookup_portable_references_response, open_connection_response,
        portable_reference_lookup_result, read_catalog_response, read_state_response, secret_patch,
        start_authentication_response, subject_reference, targeted_state_write_intent,
        validate_connection_response, value,
    },
    validation::CatalogStreamValidator,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

const ACCOUNT_TOKEN: &str = "plex-test-account-token";
const TOKEN: &str = "plex-test-server-token";

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
    assert_eq!(
        result
            .capabilities
            .expect("connection capabilities")
            .reference_lookup
            .expect("reference lookup capability")
            .reference_namespaces,
        mapping::REFERENCE_NAMESPACES.map(str::to_owned)
    );
    adapter
}

#[tokio::test]
async fn exposes_server_url_without_requesting_a_manual_token() {
    let adapter = PlexAdapter::new("test-process", CancellationToken::new());
    let description = adapter
        .describe_connection(Request::new(DescribeConnectionRequest {}))
        .await
        .unwrap()
        .into_inner();
    let Some(describe_connection_response::Outcome::Result(description)) = description.outcome
    else {
        panic!("Plex connection description failed");
    };

    assert_eq!(description.fields.len(), 1);
    assert_eq!(description.fields[0].key, "server_url");
    assert!(!description.fields[0].secret);

    let validation = adapter
        .validate_connection(Request::new(ValidateConnectionRequest {
            settings: vec![configuration("server_url", "http://localhost:32400")],
            secrets: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(validate_connection_response::Outcome::Result(validation)) = validation.outcome else {
        panic!("Plex connection validation failed");
    };
    assert!(validation.field_problems.is_empty());
}

#[tokio::test]
async fn reports_rejected_credentials_when_opening_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-plex-token", TOKEN))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
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
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::Authentication as i32
    );
    assert_eq!(failure.code, "connection_authentication_failed");
    assert_eq!(
        failure.safe_message,
        "The Plex server rejected the authentication token. Connect again to reauthenticate."
    );
    server.verify().await;
}

#[tokio::test]
async fn reports_denied_access_when_opening_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-plex-token", TOKEN))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
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
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::Authorization as i32
    );
    assert_eq!(failure.code, "connection_authorization_failed");
    assert_eq!(
        failure.safe_message,
        "The Plex server denied access for the authenticated account."
    );
    server.verify().await;
}

#[tokio::test]
async fn reports_server_missing_from_the_signed_in_account() {
    let cloud = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/resources"))
        .and(header("x-plex-token", ACCOUNT_TOKEN))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "name": "Another Plex",
                "clientIdentifier": "server-2",
                "accessToken": TOKEN,
                "provides": "server",
                "connections": []
            }])),
        )
        .expect(1)
        .mount(&cloud)
        .await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/identity"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "machineIdentifier": "server-1"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter =
        PlexAdapter::with_cloud_base_url("test-process", CancellationToken::new(), cloud.uri());
    let response = adapter
        .open_connection(Request::new(OpenConnectionRequest {
            settings: vec![configuration("server_url", &server.uri())],
            secrets: vec![SecretValue {
                key: "account_token".to_owned(),
                value: ACCOUNT_TOKEN.as_bytes().to_vec(),
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::Authorization as i32
    );
    assert_eq!(failure.code, "connection_server_not_found");
    assert_eq!(
        failure.safe_message,
        "The signed-in Plex account does not include this server."
    );
    cloud.verify().await;
    server.verify().await;
}

#[tokio::test]
async fn reports_server_resource_without_an_access_token() {
    let cloud = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/resources"))
        .and(header("x-plex-token", ACCOUNT_TOKEN))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "name": "Home Plex",
                "clientIdentifier": "server-1",
                "provides": "server",
                "connections": []
            }])),
        )
        .expect(1)
        .mount(&cloud)
        .await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/identity"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "machineIdentifier": "server-1"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter =
        PlexAdapter::with_cloud_base_url("test-process", CancellationToken::new(), cloud.uri());
    let response = adapter
        .open_connection(Request::new(OpenConnectionRequest {
            settings: vec![configuration("server_url", &server.uri())],
            secrets: vec![SecretValue {
                key: "account_token".to_owned(),
                value: ACCOUNT_TOKEN.as_bytes().to_vec(),
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::Authorization as i32
    );
    assert_eq!(failure.code, "connection_server_access_denied");
    assert_eq!(
        failure.safe_message,
        "The signed-in Plex account does not have access to this server."
    );
    cloud.verify().await;
    server.verify().await;
}

#[tokio::test]
async fn reports_malformed_server_information_when_opening_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-plex-token", TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "unexpected": true
        })))
        .expect(1)
        .mount(&server)
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
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::InvalidRemoteData as i32
    );
    assert_eq!(failure.code, "connection_invalid_response");
    assert_eq!(
        failure.safe_message,
        "The Plex server returned an invalid response."
    );
    server.verify().await;
}

#[tokio::test]
async fn reports_transport_failure_when_opening_the_server() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let server_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let adapter = PlexAdapter::new("test-process", CancellationToken::new());
    let response = adapter
        .open_connection(Request::new(OpenConnectionRequest {
            settings: vec![configuration("server_url", &server_url)],
            secrets: vec![SecretValue {
                key: "token".to_owned(),
                value: TOKEN.as_bytes().to_vec(),
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(open_connection_response::Outcome::Error(failure)) = response.outcome else {
        panic!("Plex connection unexpectedly opened");
    };

    assert_eq!(
        failure.category,
        OperationFailureCategory::Unavailable as i32
    );
    assert_eq!(failure.code, "connection_unavailable");
    assert_eq!(
        failure.safe_message,
        "The Plex server could not be reached."
    );
}

#[tokio::test]
async fn pin_authentication_opens_the_server_and_discovers_sources() {
    let cloud = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/pins"))
        .and(query_param("strong", "true"))
        .and(header(
            "x-plex-client-identifier",
            "trakkin-authentication-process",
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": 42,
            "code": "ABCD",
            "expiresIn": 300
        })))
        .expect(1)
        .mount(&cloud)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/pins/42"))
        .and(header(
            "x-plex-client-identifier",
            "trakkin-authentication-process",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 42,
            "code": "ABCD",
            "authToken": ACCOUNT_TOKEN,
            "expiresIn": 300
        })))
        .expect(1)
        .mount(&cloud)
        .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/identity"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "machineIdentifier": "server-1"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/resources"))
        .and(query_param("includeHttps", "1"))
        .and(query_param("includeRelay", "1"))
        .and(query_param("includeIPv6", "1"))
        .and(header("x-plex-token", ACCOUNT_TOKEN))
        .and(header(
            "x-plex-client-identifier",
            "trakkin-authentication-process",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "name": "Home Plex",
                "clientIdentifier": "server-1",
                "accessToken": TOKEN,
                "provides": "server",
                "connections": []
            }])),
        )
        .expect(1)
        .mount(&cloud)
        .await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-plex-token", TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "machineIdentifier": "server-1",
                "friendlyName": "Home Plex"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    mount_movie_section(&server).await;

    let authentication_adapter = PlexAdapter::with_cloud_base_url(
        "authentication-process",
        CancellationToken::new(),
        cloud.uri(),
    );
    let started = authentication_adapter
        .start_authentication(Request::new(StartAuthenticationRequest {
            method: "plex/pin".to_owned(),
            values: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(start_authentication_response::Outcome::Result(started)) = started.outcome else {
        panic!("Plex PIN authentication did not start");
    };
    assert_eq!(started.status, AuthenticationStatus::InputRequired as i32);
    assert_eq!(
        started
            .prompt
            .as_ref()
            .map(|prompt| prompt.user_code.as_str()),
        Some("ABCD")
    );

    let completed = authentication_adapter
        .continue_authentication(Request::new(ContinueAuthenticationRequest {
            authentication_id: started.authentication_id,
            values: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(continue_authentication_response::Outcome::Result(completed)) = completed.outcome
    else {
        panic!("Plex PIN authentication did not complete");
    };
    assert_eq!(completed.status, AuthenticationStatus::Completed as i32);
    let token = completed
        .secret_patches
        .iter()
        .find_map(|patch| match (patch.key.as_str(), patch.patch.as_ref()) {
            ("account_token", Some(secret_patch::Patch::Set(value))) => Some(value.clone()),
            _ => None,
        })
        .expect("completed Plex authentication did not return an account token");
    assert_eq!(token, ACCOUNT_TOKEN.as_bytes());
    let client_identifier = completed
        .secret_patches
        .into_iter()
        .find_map(|patch| match (patch.key.as_str(), patch.patch) {
            ("client_identifier", Some(secret_patch::Patch::Set(value))) => Some(value),
            _ => None,
        })
        .expect("completed Plex authentication did not preserve the client identifier");

    let discovery_adapter = PlexAdapter::with_cloud_base_url(
        "discovery-process",
        CancellationToken::new(),
        cloud.uri(),
    );
    let opened = discovery_adapter
        .open_connection(Request::new(OpenConnectionRequest {
            settings: vec![configuration("server_url", &server.uri())],
            secrets: vec![
                SecretValue {
                    key: "account_token".to_owned(),
                    value: token,
                },
                SecretValue {
                    key: "client_identifier".to_owned(),
                    value: client_identifier,
                },
            ],
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(open_connection_response::Outcome::Result(opened)) = opened.outcome else {
        panic!("Plex connection did not open after authentication");
    };
    assert!(opened.secret_patches.iter().any(|patch| {
        patch.key == "token"
            && patch.patch == Some(secret_patch::Patch::Set(TOKEN.as_bytes().to_vec()))
    }));
    assert!(opened.secret_patches.iter().any(|patch| {
        patch.key == "account_token" && matches!(patch.patch, Some(secret_patch::Patch::Remove(_)))
    }));

    let discovered = discovery_adapter
        .discover_sources(Request::new(DiscoverSourcesRequest {}))
        .await
        .unwrap()
        .into_inner();
    let Some(discover_sources_response::Outcome::Result(discovered)) = discovered.outcome else {
        panic!("Plex sources were not discovered after authentication");
    };
    assert_eq!(discovered.sources.len(), 1);
    assert_eq!(discovered.sources[0].display_name, "Movies");
    cloud.verify().await;
    server.verify().await;
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
                    "agent": "tv.plex.agents.movie",
                    "type": "movie",
                    "updatedAt": 200
                }]
            }
        })))
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_nonadvancing_movie_pages(server: &MockServer) {
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
                        "title": "First"
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
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/1/all"))
        .and(query_param("includeGuids", "1"))
        .and(header("x-plex-container-start", "2"))
        .and(header("x-plex-container-size", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "size": 1,
                "offset": 0,
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
        .mount(server)
        .await;
}

#[tokio::test]
async fn rejects_a_nonadvancing_catalog_page() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;
    mount_nonadvancing_movie_pages(&server).await;

    let mut stream = adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"catalog-pagination-test".to_vec(),
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

    assert!(!events.iter().any(|event| matches!(
        event.event,
        Some(read_catalog_response::Event::Completed(_))
    )));
    let Some(read_catalog_response::Event::Failed(failed)) = &events
        .last()
        .expect("catalog stream has a terminal event")
        .event
    else {
        panic!("catalog stream did not end in failure");
    };
    assert_eq!(
        failed.error.as_ref().expect("catalog failure").code,
        "catalog_pagination_invalid"
    );
    server.verify().await;
}

#[tokio::test]
async fn rejects_a_nonadvancing_state_page() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;
    mount_nonadvancing_movie_pages(&server).await;

    let mut stream = adapter
        .read_state(Request::new(ReadStateRequest {
            operation_id: b"state-pagination-test".to_vec(),
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

    assert!(
        !events
            .iter()
            .any(|event| matches!(event.event, Some(read_state_response::Event::Completed(_))))
    );
    let Some(read_state_response::Event::Failed(failed)) = &events
        .last()
        .expect("state stream has a terminal event")
        .event
    else {
        panic!("state stream did not end in failure");
    };
    assert_eq!(
        failed.error.as_ref().expect("state failure").code,
        "state_pagination_invalid"
    );
    server.verify().await;
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
async fn source_refresh_preserves_the_plex_http_failure() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    Mock::given(method("GET"))
        .and(path("/library/sections/all"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let error = match adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"failed-source-refresh".to_vec(),
            source_key: Some(mapping::source_key("missing")),
            mode: ReadMode::Full as i32,
            prior_cursor: Vec::new(),
            preferred_batch_size: 10,
        }))
        .await
    {
        Ok(_) => panic!("failed Plex source refresh unexpectedly started a read"),
        Err(error) => error,
    };

    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("503 Service Unavailable"));
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
                        "Guid": [
                            { "id": "imdb://tt0000010" },
                            { "id": "tmdb://10" },
                            { "id": "tvdb://20" }
                        ]
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
    assert_eq!(
        first_batch.item_upserts[0]
            .portable_reference_candidates
            .len(),
        4
    );
    assert_eq!(
        first_batch.item_upserts[0]
            .recommended_mapping_roots
            .iter()
            .map(|reference| reference.namespace.as_str())
            .collect::<Vec<_>>(),
        vec![
            mapping::IMDB_REFERENCE_NAMESPACE,
            mapping::TMDB_REFERENCE_NAMESPACE,
            mapping::TVDB_REFERENCE_NAMESPACE,
        ]
    );
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
async fn applies_show_ordering_with_section_fallback_to_catalog_and_lookup() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;

    Mock::given(method("GET"))
        .and(path("/library/sections/all"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Directory": [{
                    "key": "2",
                    "uuid": "series-2",
                    "title": "Series",
                    "agent": "tv.plex.agents.series",
                    "type": "show",
                    "updatedAt": 300
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/2/prefs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Setting": [
                    { "id": "includeAdult", "value": false },
                    { "id": "showOrdering", "value": "tmdbAiring" }
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/2/all"))
        .and(query_param("includeGuids", "1"))
        .and(query_param("type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "size": 2,
                "offset": 0,
                "totalSize": 2,
                "Metadata": [
                    {
                        "ratingKey": "50",
                        "key": "/library/metadata/50",
                        "guid": "plex://show/default",
                        "type": "show",
                        "title": "Section Default",
                        "showOrdering": null,
                        "Guid": [
                            { "id": "imdb://tt0000050" },
                            { "id": "tmdb://50" },
                            { "id": "tvdb://150" }
                        ]
                    },
                    {
                        "ratingKey": "51",
                        "key": "/library/metadata/51",
                        "guid": "plex://show/override",
                        "type": "show",
                        "title": "Show Override",
                        "showOrdering": "tvdbDvd",
                        "Guid": [
                            { "id": "tmdb://51" },
                            { "id": "tvdb://151" }
                        ]
                    }
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/2/all"))
        .and(query_param("includeGuids", "1"))
        .and(query_param("type", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Metadata": [{
                    "ratingKey": "70",
                    "key": "/library/metadata/70",
                    "parentRatingKey": "50",
                    "guid": "plex://season/default-1",
                    "type": "season",
                    "title": "Season 1",
                    "parentTitle": "Section Default",
                    "index": 1
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/2/all"))
        .and(query_param("includeGuids", "1"))
        .and(query_param("type", "4"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Metadata": [{
                    "ratingKey": "80",
                    "key": "/library/metadata/80",
                    "parentRatingKey": "70",
                    "grandparentRatingKey": "50",
                    "guid": "plex://episode/default-1-1",
                    "type": "episode",
                    "title": "\n",
                    "parentTitle": "Season 1",
                    "grandparentTitle": "Section Default",
                    "index": 1,
                    "parentIndex": 1,
                    "Guid": [{
                        "id": format!("org.example.agent://{}", "x".repeat(1_025))
                    }]
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/2/all"))
        .and(query_param("guid", "tvdb://151"))
        .and(query_param("includeGuids", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Metadata": [{
                    "ratingKey": "51",
                    "key": "/library/metadata/51",
                    "guid": "plex://show/override",
                    "type": "show",
                    "title": "Show Override",
                    "showOrdering": "tvdbDvd",
                    "Guid": [
                        { "id": "tmdb://51" },
                        { "id": "tvdb://151" }
                    ]
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut stream = adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"series-catalog".to_vec(),
            source_key: Some(mapping::source_key("2")),
            mode: ReadMode::Full as i32,
            prior_cursor: Vec::new(),
            preferred_batch_size: 10,
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
    assert_eq!(events.len(), 4);
    let Some(read_catalog_response::Event::Batch(batch)) = &events[0].event else {
        panic!("series catalog did not begin with a batch");
    };
    assert_eq!(
        batch.item_upserts[0]
            .recommended_mapping_roots
            .iter()
            .map(|reference| reference.namespace.as_str())
            .collect::<Vec<_>>(),
        vec![mapping::TMDB_REFERENCE_NAMESPACE]
    );
    assert_eq!(
        batch.item_upserts[1]
            .recommended_mapping_roots
            .iter()
            .map(|reference| reference.namespace.as_str())
            .collect::<Vec<_>>(),
        vec![mapping::TVDB_REFERENCE_NAMESPACE]
    );
    let Some(read_catalog_response::Event::Batch(season_batch)) = &events[1].event else {
        panic!("series catalog did not include a season batch");
    };
    assert_eq!(
        season_batch.relation_upserts[0].parent_key,
        Some(mapping::relation_key("50"))
    );
    let Some(read_catalog_response::Event::Batch(episode_batch)) = &events[2].event else {
        panic!("series catalog did not include an episode batch");
    };
    assert_eq!(
        episode_batch.item_upserts[0].display_name,
        "Section Default - Season 1 - Untitled Plex item 80"
    );
    assert_eq!(
        episode_batch.item_upserts[0]
            .portable_reference_candidates
            .len(),
        1
    );
    assert_eq!(
        episode_batch.relation_upserts[0].parent_key,
        Some(mapping::relation_key("70"))
    );

    let response = adapter
        .lookup_portable_references(Request::new(LookupPortableReferencesRequest {
            operation_id: b"series-lookup".to_vec(),
            references: vec![PortableReference {
                namespace: mapping::TVDB_REFERENCE_NAMESPACE.to_owned(),
                value: b"series/151".to_vec(),
            }],
            source_key: Some(mapping::source_key("2")),
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(lookup_portable_references_response::Outcome::Result(result)) = response.outcome
    else {
        panic!("series reference lookup failed");
    };
    let Some(portable_reference_lookup_result::Outcome::Matched(matched)) =
        &result.results[0].outcome
    else {
        panic!("series reference did not resolve");
    };
    let provider_item = matched
        .candidate
        .as_ref()
        .and_then(|candidate| candidate.provider_item.as_ref())
        .expect("matched series provider item");
    assert_eq!(
        provider_item
            .recommended_mapping_roots
            .iter()
            .map(|reference| reference.namespace.as_str())
            .collect::<Vec<_>>(),
        vec![mapping::TVDB_REFERENCE_NAMESPACE]
    );
    server.verify().await;
}

#[tokio::test]
async fn does_not_recommend_routes_for_unsupported_agents() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;

    Mock::given(method("GET"))
        .and(path("/library/sections/all"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Directory": [{
                    "key": "3",
                    "uuid": "legacy-movies-3",
                    "title": "Legacy Movies",
                    "agent": "com.plexapp.agents.imdb",
                    "type": "movie",
                    "updatedAt": 400
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/library/sections/3/all"))
        .and(query_param("includeGuids", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "size": 1,
                "offset": 0,
                "totalSize": 1,
                "Metadata": [{
                    "ratingKey": "60",
                    "key": "/library/metadata/60",
                    "guid": "plex://movie/legacy",
                    "type": "movie",
                    "title": "Legacy",
                    "Guid": [
                        { "id": "imdb://tt0000060" },
                        { "id": "tmdb://60" },
                        { "id": "tvdb://160" }
                    ]
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut stream = adapter
        .read_catalog(Request::new(ReadCatalogRequest {
            operation_id: b"unsupported-agent-catalog".to_vec(),
            source_key: Some(mapping::source_key("3")),
            mode: ReadMode::Full as i32,
            prior_cursor: Vec::new(),
            preferred_batch_size: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    let first_event = stream.next().await.unwrap().unwrap();
    let Some(read_catalog_response::Event::Batch(batch)) = first_event.event else {
        panic!("unsupported-agent catalog did not begin with a batch");
    };
    assert_eq!(batch.item_upserts[0].portable_reference_candidates.len(), 4);
    assert!(batch.item_upserts[0].recommended_mapping_roots.is_empty());
    while stream.next().await.is_some() {}
    server.verify().await;
}

#[tokio::test]
async fn looks_up_canonical_portable_references_through_plex_guids() {
    let server = MockServer::start().await;
    let adapter = open_adapter(&server).await;
    mount_movie_section(&server).await;

    Mock::given(method("GET"))
        .and(path("/library/sections/1/all"))
        .and(query_param("guid", "tvdb://123"))
        .and(query_param("includeGuids", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MediaContainer": {
                "Metadata": [
                    {
                        "ratingKey": "41",
                        "key": "/library/metadata/41",
                        "guid": "plex://movie/collision",
                        "type": "movie",
                        "title": "Wrong database subset",
                        "Guid": [{ "id": "tvdb://123" }]
                    },
                    {
                        "ratingKey": "42",
                        "key": "/library/metadata/42",
                        "guid": "plex://show/example",
                        "type": "show",
                        "title": "Example",
                        "Guid": [{ "id": "tvdb://123" }]
                    }
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let response = adapter
        .lookup_portable_references(Request::new(LookupPortableReferencesRequest {
            operation_id: b"reference-lookup".to_vec(),
            references: vec![PortableReference {
                namespace: mapping::TVDB_REFERENCE_NAMESPACE.to_owned(),
                value: b"series/123".to_vec(),
            }],
            source_key: Some(mapping::source_key("1")),
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(lookup_portable_references_response::Outcome::Result(result)) = response.outcome
    else {
        panic!("portable reference lookup failed");
    };
    let Some(portable_reference_lookup_result::Outcome::Matched(matched)) =
        &result.results[0].outcome
    else {
        panic!("portable reference did not resolve to one subset-matched item");
    };
    assert_eq!(
        matched
            .candidate
            .as_ref()
            .and_then(|candidate| candidate.provider_item.as_ref())
            .map(|item| item.display_name.as_str()),
        Some("Example")
    );
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

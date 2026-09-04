use std::{process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};
use tonic::{Code, Request, Streaming, transport::Channel};
use trakkin_provider_fixture::scenarios::{
    ADVERSARIAL, DUAL_CONNECTION_ISOLATION, EMPTY_TARGETED_RECEIVER,
};
use trakkin_provider_sdk::{
    BOOTSTRAP_VERSION, InvocationContext, LaunchRequest, LaunchToken, ReadyMessage,
    supported_protocol_range,
};
use trakkin_provider_sdk::{
    v1::{
        AuthenticationStatus, ConfigurationValue, ContinueAuthenticationRequest, CoordinateBacking,
        HandshakeRequest, HealthRequest, LookupPortableReferencesRequest, OpenConnectionRequest,
        OpenConnectionResult, OperationFailure, PortableEndpoint, PortableReference,
        ReadAssetRequest, ReadAssetResponse, ReadCatalogRequest, ReadCatalogResponse, ReadMode,
        ReadStateRequest, ReadStateResponse, ResolvePortableEndpointsRequest, RetryDisposition,
        SecretValue, ShutdownRequest, StartAuthenticationRequest, Value,
        adapter_service_client::AdapterServiceClient, cancel_authentication_response,
        continue_authentication_response, describe_connection_response, discover_sources_response,
        list_authentication_methods_response, lookup_portable_references_response,
        open_connection_response, portable_endpoint_resolution, portable_reference_lookup_result,
        read_asset_response, read_catalog_response, read_state_response,
        resolve_portable_endpoints_response, start_authentication_response,
        validate_connection_response, value,
    },
    validation::{
        CatalogStreamValidator, StateStreamValidator, asset_response, resolve_endpoints_response,
    },
};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);

struct FixtureProcess {
    child: Child,
    client: AdapterServiceClient<Channel>,
    launch_token: LaunchToken,
    process_instance_id: String,
    stderr_task: JoinHandle<Vec<String>>,
}

impl FixtureProcess {
    async fn launch(process_instance_id: &str) -> Self {
        let launch = LaunchRequest {
            bootstrap_version: BOOTSTRAP_VERSION,
            process_instance_id: process_instance_id.to_owned(),
            bind_address: "127.0.0.1:0".to_owned(),
            launch_token: format!("launch-token-{process_instance_id}"),
        };
        let mut child = Command::new(env!("CARGO_BIN_EXE_trakkin-provider-fixture"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            let mut diagnostics = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                diagnostics.push(line);
            }
            diagnostics
        });
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(format!("{}\n", serde_json::to_string(&launch).unwrap()).as_bytes())
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();

        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        timeout(RPC_TIMEOUT, stdout.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let ready = serde_json::from_str::<ReadyMessage>(&line).unwrap();
        assert_eq!(ready.bootstrap_version, BOOTSTRAP_VERSION);
        assert_eq!(ready.process_instance_id, process_instance_id);
        assert_eq!(ready.launch_token, launch.launch_token);

        let client = AdapterServiceClient::connect(format!("http://{}", ready.address))
            .await
            .unwrap();
        Self {
            child,
            client,
            launch_token: LaunchToken::new(&launch.launch_token).unwrap(),
            process_instance_id: process_instance_id.to_owned(),
            stderr_task,
        }
    }

    fn signed<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        self.launch_token.apply(&mut request);
        InvocationContext::new(
            &format!("fixture:{}", self.process_instance_id),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            Some("fixture=test"),
        )
        .unwrap()
        .apply(&mut request);
        request
    }

    async fn handshake(&mut self) {
        let request = self.signed(HandshakeRequest {
            supported_protocol: Some(supported_protocol_range()),
            host_version: "fixture-contract-test".to_owned(),
            process_instance_id: self.process_instance_id.clone(),
        });
        let response = timeout(RPC_TIMEOUT, self.client.handshake(request))
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(response.adapter_key, "dev.trakkin.fixture");
        assert_eq!(response.process_instance_id, self.process_instance_id);
        assert_eq!(response.selected_protocol.unwrap().minor, 0);
    }

    async fn open(
        &mut self,
        scenario: &str,
        revision: &str,
        fault: &str,
        instance: &str,
        secret: &str,
    ) -> Result<OpenConnectionResult, OperationFailure> {
        let request = self.signed(OpenConnectionRequest {
            settings: [
                ("scenario", scenario),
                ("revision", revision),
                ("fault", fault),
                ("instance", instance),
            ]
            .into_iter()
            .map(|(key, text)| configuration(key, text))
            .collect(),
            secrets: vec![SecretValue {
                key: "fixture_token".to_owned(),
                value: secret.as_bytes().to_vec(),
            }],
        });
        let response = timeout(RPC_TIMEOUT, self.client.open_connection(request))
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        match response.outcome.expect("fixture returned an open outcome") {
            open_connection_response::Outcome::Result(result) => Ok(result),
            open_connection_response::Outcome::Error(error) => Err(error),
        }
    }

    async fn catalog_stream(
        &mut self,
        operation_id: &[u8],
        source: &str,
        mode: ReadMode,
        prior_cursor: Vec<u8>,
    ) -> Result<Streaming<ReadCatalogResponse>, tonic::Status> {
        let request = self.signed(ReadCatalogRequest {
            operation_id: operation_id.to_vec(),
            source_key: Some(key(source)),
            mode: mode as i32,
            prior_cursor,
            preferred_batch_size: 100,
        });
        self.client
            .read_catalog(request)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn state_stream(
        &mut self,
        operation_id: &[u8],
        source: &str,
        mode: ReadMode,
        prior_cursor: Vec<u8>,
    ) -> Result<Streaming<ReadStateResponse>, tonic::Status> {
        let request = self.signed(ReadStateRequest {
            operation_id: operation_id.to_vec(),
            source_key: Some(key(source)),
            mode: mode as i32,
            prior_cursor,
            preferred_batch_size: 100,
        });
        self.client
            .read_state(request)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn read_asset(
        &mut self,
        source: &str,
        provider_item: &str,
        asset: &str,
    ) -> ReadAssetResponse {
        let request = self.signed(ReadAssetRequest {
            operation_id: b"asset-read".to_vec(),
            source_key: Some(key(source)),
            provider_item_key: Some(key(provider_item)),
            asset_key: Some(key(asset)),
            maximum_bytes: 1024,
        });
        timeout(RPC_TIMEOUT, self.client.read_asset(request))
            .await
            .unwrap()
            .unwrap()
            .into_inner()
    }

    async fn shutdown(mut self) {
        let request = self.signed(ShutdownRequest { grace_period: None });
        timeout(RPC_TIMEOUT, self.client.shutdown(request))
            .await
            .unwrap()
            .unwrap();
        let status = timeout(RPC_TIMEOUT, self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        let diagnostics = timeout(RPC_TIMEOUT, self.stderr_task)
            .await
            .unwrap()
            .unwrap();
        assert!(!diagnostics.is_empty());
        for line in &diagnostics {
            serde_json::from_str::<serde_json::Value>(line).unwrap();
            assert!(!line.contains("fixture-secret"));
            assert!(!line.contains("launch-token-"));
        }
        assert!(
            diagnostics
                .iter()
                .any(|line| line.contains("provider.logging.initialized"))
        );
        assert!(diagnostics.iter().any(|line| line.contains("provider.rpc")));
        assert!(
            diagnostics
                .iter()
                .any(|line| { line.contains(&format!("fixture:{}", self.process_instance_id)) })
        );
        assert!(diagnostics.iter().any(|line| {
            line.contains("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        }));
        assert!(diagnostics.iter().any(|line| line.contains("fixture=test")));
    }
}

fn configuration(key: &str, text: &str) -> ConfigurationValue {
    ConfigurationValue {
        key: key.to_owned(),
        value: Some(Value {
            value: Some(value::Value::Text(text.to_owned())),
        }),
    }
}

fn key(value: &str) -> trakkin_provider_sdk::v1::Key {
    trakkin_provider_sdk::v1::Key {
        namespace: "fixture".to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

fn reference(value: &str) -> PortableReference {
    PortableReference {
        namespace: "example.media".to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

async fn collect_catalog(mut stream: Streaming<ReadCatalogResponse>) -> Vec<ReadCatalogResponse> {
    let mut events = Vec::new();
    while let Some(event) = timeout(RPC_TIMEOUT, stream.message())
        .await
        .unwrap()
        .unwrap()
    {
        events.push(event);
    }
    events
}

async fn collect_state(mut stream: Streaming<ReadStateResponse>) -> Vec<ReadStateResponse> {
    let mut events = Vec::new();
    while let Some(event) = timeout(RPC_TIMEOUT, stream.message())
        .await
        .unwrap()
        .unwrap()
    {
        events.push(event);
    }
    events
}

fn validate_catalog(events: &[ReadCatalogResponse]) {
    let mut validator = CatalogStreamValidator::default();
    for event in events {
        validator.accept(event).unwrap();
    }
    validator.finish().unwrap();
}

#[test]
fn provider_metadata_matches_package() {
    let metadata =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/provider.json")).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();

    assert_eq!(metadata["schema"], "trakkin.provider/v1");
    assert_eq!(metadata["id"], "dev.trakkin.fixture");
    assert_eq!(metadata["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_token_auth_and_empty_receiver_flow_cross_process_boundary() {
    let mut fixture = FixtureProcess::launch("core-flow").await;
    let error = fixture
        .client
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    fixture.handshake().await;

    let request = fixture.signed(trakkin_provider_sdk::v1::DescribeConnectionRequest {});
    let description = fixture
        .client
        .describe_connection(request)
        .await
        .unwrap()
        .into_inner();
    let Some(describe_connection_response::Outcome::Result(_description)) = description.outcome
    else {
        panic!("fixture did not return a connection description");
    };
    let request = fixture.signed(trakkin_provider_sdk::v1::ValidateConnectionRequest {
        settings: [
            ("scenario", EMPTY_TARGETED_RECEIVER),
            ("revision", "v1"),
            ("fault", "unknown-fault"),
            ("instance", "alpha"),
        ]
        .into_iter()
        .map(|(key, text)| configuration(key, text))
        .collect(),
        secrets: vec![SecretValue {
            key: "fixture_token".to_owned(),
            value: b"fixture-secret".to_vec(),
        }],
    });
    let validation = fixture
        .client
        .validate_connection(request)
        .await
        .unwrap()
        .into_inner();
    let Some(validate_connection_response::Outcome::Result(validation)) = validation.outcome else {
        panic!("fixture did not return connection validation");
    };
    assert!(
        validation
            .field_problems
            .iter()
            .any(|problem| problem.path == "fault")
    );

    let request = fixture.signed(trakkin_provider_sdk::v1::ListAuthenticationMethodsRequest {});
    let methods = fixture
        .client
        .list_authentication_methods(request)
        .await
        .unwrap()
        .into_inner();
    let Some(list_authentication_methods_response::Outcome::Result(_methods)) = methods.outcome
    else {
        panic!("fixture did not return authentication methods");
    };

    let request = fixture.signed(StartAuthenticationRequest {
        method: "fixture/device".to_owned(),
        values: Vec::new(),
    });
    let step = fixture
        .client
        .start_authentication(request)
        .await
        .unwrap()
        .into_inner();
    let Some(start_authentication_response::Outcome::Result(step)) = step.outcome else {
        panic!("fixture did not start authentication");
    };
    assert_eq!(step.status, AuthenticationStatus::InputRequired as i32);
    let request = fixture.signed(ContinueAuthenticationRequest {
        authentication_id: step.authentication_id,
        values: vec![configuration("code", "approved")],
    });
    let step = fixture
        .client
        .continue_authentication(request)
        .await
        .unwrap()
        .into_inner();
    let Some(continue_authentication_response::Outcome::Result(step)) = step.outcome else {
        panic!("fixture did not continue authentication");
    };
    assert_eq!(step.status, AuthenticationStatus::Completed as i32);

    for (method, expected) in [
        ("fixture/wait", AuthenticationStatus::Waiting),
        ("fixture/expire", AuthenticationStatus::Expired),
        ("fixture/cancel", AuthenticationStatus::Waiting),
    ] {
        let request = fixture.signed(StartAuthenticationRequest {
            method: method.to_owned(),
            values: Vec::new(),
        });
        let step = fixture
            .client
            .start_authentication(request)
            .await
            .unwrap()
            .into_inner();
        let Some(start_authentication_response::Outcome::Result(step)) = step.outcome else {
            panic!("fixture did not return authentication progress");
        };
        assert_eq!(step.status, expected as i32);
    }
    let request = fixture.signed(StartAuthenticationRequest {
        method: "fixture/deny".to_owned(),
        values: Vec::new(),
    });
    let denied = fixture
        .client
        .start_authentication(request)
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        denied.outcome,
        Some(start_authentication_response::Outcome::Error(_))
    ));
    let request = fixture.signed(trakkin_provider_sdk::v1::CancelAuthenticationRequest {
        authentication_id: "auth-cancel".to_owned(),
    });
    let cancelled = fixture
        .client
        .cancel_authentication(request)
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        cancelled.outcome,
        Some(cancel_authentication_response::Outcome::Result(_))
    ));

    let response = fixture
        .open(
            EMPTY_TARGETED_RECEIVER,
            "v1",
            "none",
            "alpha",
            "fixture-secret",
        )
        .await;
    assert!(response.is_ok());
    let request = fixture.signed(trakkin_provider_sdk::v1::DiscoverSourcesRequest {});
    let discovery = fixture
        .client
        .discover_sources(request)
        .await
        .unwrap()
        .into_inner();
    let Some(discover_sources_response::Outcome::Result(_discovery)) = discovery.outcome else {
        panic!("fixture source discovery failed");
    };
    let events = collect_catalog(
        fixture
            .catalog_stream(
                b"empty-receiver-catalog",
                "source.receiver.empty",
                ReadMode::Full,
                Vec::new(),
            )
            .await
            .unwrap(),
    )
    .await;
    validate_catalog(&events);
    let batch = match events[0].event.as_ref().unwrap() {
        read_catalog_response::Event::Batch(batch) => batch,
        _ => panic!("expected catalog batch"),
    };
    assert!(batch.item_upserts.is_empty());

    let requested = reference("media/quiet-signal");
    let request = fixture.signed(LookupPortableReferencesRequest {
        operation_id: b"lookup-empty-receiver".to_vec(),
        references: vec![requested],
    });
    let lookup = fixture
        .client
        .lookup_portable_references(request)
        .await
        .unwrap()
        .into_inner();
    let Some(lookup_portable_references_response::Outcome::Result(lookup)) = lookup.outcome else {
        panic!("fixture lookup failed");
    };
    assert!(matches!(
        lookup.results[0].outcome,
        Some(portable_reference_lookup_result::Outcome::Matched(_))
    ));

    let requested = PortableEndpoint {
        reference: Some(reference("media/quiet-signal")),
        selector: "segment:1..12".to_owned(),
    };
    let request = fixture.signed(ResolvePortableEndpointsRequest {
        operation_id: b"resolve-empty-receiver".to_vec(),
        endpoints: vec![requested.clone()],
        maximum_response_bytes: 4096,
    });
    let resolution = fixture
        .client
        .resolve_portable_endpoints(request)
        .await
        .unwrap()
        .into_inner();
    resolve_endpoints_response(std::slice::from_ref(&requested), &resolution, 4096).unwrap();
    let Some(resolve_portable_endpoints_response::Outcome::Result(resolution)) = resolution.outcome
    else {
        panic!("fixture endpoint resolution failed");
    };
    let candidate = match resolution.results[0].outcome.as_ref().unwrap() {
        portable_endpoint_resolution::Outcome::Matched(matched) => {
            matched.candidate.as_ref().unwrap()
        }
        _ => panic!("expected matched endpoint"),
    };
    assert_eq!(
        candidate.binding.as_ref().unwrap().backing,
        CoordinateBacking::Aggregate as i32
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_faults_have_deterministic_contract_outcomes() {
    for fault in [
        "read.stale-cursor",
        "read.cancel-after-batch",
        "read.retryable-after-batch",
        "read.fatal-after-batch",
        "read.malformed-catalog",
    ] {
        let mut fixture =
            FixtureProcess::launch(&format!("fault-{}", fault.replace('.', "-"))).await;
        let response = fixture
            .open(ADVERSARIAL, "v2", fault, "alpha", "fixture-secret")
            .await;
        assert!(response.is_ok());
        let events = collect_catalog(
            fixture
                .catalog_stream(
                    b"fault-catalog",
                    "source.adversarial.catalog",
                    ReadMode::Full,
                    Vec::new(),
                )
                .await
                .unwrap(),
        )
        .await;
        match fault {
            "read.stale-cursor" => {
                assert!(matches!(
                    events[0].event,
                    Some(read_catalog_response::Event::Failed(_))
                ));
                let state_events = collect_state(
                    fixture
                        .state_stream(
                            b"stale-state",
                            "source.adversarial.state",
                            ReadMode::Full,
                            Vec::new(),
                        )
                        .await
                        .unwrap(),
                )
                .await;
                assert!(matches!(
                    state_events[0].event,
                    Some(read_state_response::Event::Failed(_))
                ));
            }
            "read.cancel-after-batch" => assert!(matches!(
                events.last().unwrap().event,
                Some(read_catalog_response::Event::Cancelled(_))
            )),
            "read.retryable-after-batch" => match events.last().unwrap().event.as_ref().unwrap() {
                read_catalog_response::Event::Failed(failed) => {
                    assert_eq!(
                        RetryDisposition::try_from(
                            failed
                                .error
                                .as_ref()
                                .unwrap()
                                .retry
                                .as_ref()
                                .unwrap()
                                .disposition,
                        )
                        .unwrap(),
                        RetryDisposition::Retryable,
                    )
                }
                _ => panic!("expected retryable failure"),
            },
            "read.fatal-after-batch" => match events.last().unwrap().event.as_ref().unwrap() {
                read_catalog_response::Event::Failed(failed) => {
                    assert_eq!(
                        RetryDisposition::try_from(
                            failed
                                .error
                                .as_ref()
                                .unwrap()
                                .retry
                                .as_ref()
                                .unwrap()
                                .disposition,
                        )
                        .unwrap(),
                        RetryDisposition::NotRetryable,
                    )
                }
                _ => panic!("expected fatal failure"),
            },
            "read.malformed-catalog" => {
                assert!(
                    CatalogStreamValidator::default()
                        .accept(&events[0])
                        .is_err()
                )
            }
            _ => unreachable!(),
        }
        fixture.shutdown().await;
    }

    let mut fixture = FixtureProcess::launch("fault-malformed-state").await;
    fixture
        .open(
            ADVERSARIAL,
            "v2",
            "read.malformed-state",
            "alpha",
            "fixture-secret",
        )
        .await
        .unwrap();
    let events = collect_state(
        fixture
            .state_stream(
                b"malformed-state",
                "source.adversarial.state",
                ReadMode::Full,
                Vec::new(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert!(StateStreamValidator::default().accept(&events[0]).is_err());
    fixture.shutdown().await;

    let mut fixture = FixtureProcess::launch("fault-asset-limit").await;
    fixture
        .open(
            ADVERSARIAL,
            "v1",
            "asset.over-limit",
            "alpha",
            "fixture-secret",
        )
        .await
        .unwrap();
    let requested = [
        reference("missing"),
        reference("duplicate"),
        reference("other"),
    ];
    let request = fixture.signed(LookupPortableReferencesRequest {
        operation_id: b"adversarial-lookup".to_vec(),
        references: requested.to_vec(),
    });
    let lookup = fixture
        .client
        .lookup_portable_references(request)
        .await
        .unwrap()
        .into_inner();
    let Some(lookup_portable_references_response::Outcome::Result(lookup)) = lookup.outcome else {
        panic!("fixture lookup failed");
    };
    assert!(matches!(
        lookup.results[0].outcome,
        Some(portable_reference_lookup_result::Outcome::NotFound(_))
    ));
    assert!(matches!(
        lookup.results[1].outcome,
        Some(portable_reference_lookup_result::Outcome::Ambiguous(_))
    ));
    assert!(matches!(
        lookup.results[2].outcome,
        Some(portable_reference_lookup_result::Outcome::Unsupported(_))
    ));
    let asset = fixture
        .read_asset(
            "source.adversarial.catalog",
            "adversarial.item.left",
            "asset.adversarial.poster",
        )
        .await;
    let Some(read_asset_response::Outcome::Error(error)) = asset.outcome else {
        panic!("fixture asset fault succeeded");
    };
    assert_eq!(error.code, "asset_too_large");
    fixture.shutdown().await;

    let mut fixture = FixtureProcess::launch("fault-secret-redaction").await;
    let response = fixture
        .open(
            ADVERSARIAL,
            "v1",
            "auth.secret-in-error",
            "alpha",
            "fixture-secret",
        )
        .await;
    let error = response.unwrap_err();
    assert_eq!(error.code, "authentication_failed");
    assert!(!error.safe_message.contains("fixture-secret"));
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_and_dual_process_state_are_isolated() {
    let mut alpha = FixtureProcess::launch("isolation-alpha").await;
    let mut beta = FixtureProcess::launch("isolation-beta").await;
    assert!(
        alpha
            .open(
                DUAL_CONNECTION_ISOLATION,
                "v1",
                "read.hang-after-heartbeat",
                "alpha",
                "fixture-secret-alpha",
            )
            .await
            .is_ok()
    );
    assert!(
        beta.open(
            DUAL_CONNECTION_ISOLATION,
            "v2",
            "none",
            "beta",
            "fixture-secret-alpha",
        )
        .await
        .is_err()
    );
    assert!(
        beta.open(
            DUAL_CONNECTION_ISOLATION,
            "v2",
            "none",
            "beta",
            "fixture-secret-beta",
        )
        .await
        .is_ok()
    );

    let operation_id = b"alpha-hanging-read";
    let mut alpha_stream = alpha
        .catalog_stream(
            operation_id,
            "source.alpha.catalog",
            ReadMode::Full,
            Vec::new(),
        )
        .await
        .unwrap();
    let heartbeat = timeout(RPC_TIMEOUT, alpha_stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        heartbeat.event,
        Some(read_catalog_response::Event::Heartbeat(_))
    ));
    let duplicate = alpha
        .catalog_stream(
            operation_id,
            "source.alpha.catalog",
            ReadMode::Full,
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists);
    let request = alpha.signed(trakkin_provider_sdk::v1::CancelOperationRequest {
        operation_id: operation_id.to_vec(),
    });
    alpha.client.cancel_operation(request).await.unwrap();
    let cancelled = timeout(RPC_TIMEOUT, alpha_stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        cancelled.event,
        Some(read_catalog_response::Event::Cancelled(_))
    ));

    let prior = format!("{DUAL_CONNECTION_ISOLATION}/source.beta.catalog/catalog/v1").into_bytes();
    let beta_events = collect_catalog(
        beta.catalog_stream(
            b"beta-update",
            "source.beta.catalog",
            ReadMode::Incremental,
            prior,
        )
        .await
        .unwrap(),
    )
    .await;
    validate_catalog(&beta_events);
    let batch = match beta_events[0].event.as_ref().unwrap() {
        read_catalog_response::Event::Batch(batch) => batch,
        _ => panic!("expected beta update batch"),
    };
    assert_eq!(batch.item_deletes, vec![key("media.beta.item.2")]);

    let alpha_asset = alpha
        .read_asset(
            "source.alpha.catalog",
            "media.alpha.root",
            "asset.alpha.poster",
        )
        .await;
    let beta_asset = beta
        .read_asset(
            "source.beta.catalog",
            "media.beta.root",
            "asset.beta.poster",
        )
        .await;
    asset_response(&alpha_asset, 1024, &["image/jpeg".to_owned()]).unwrap();
    asset_response(&beta_asset, 1024, &["image/jpeg".to_owned()]).unwrap();
    let Some(read_asset_response::Outcome::Result(alpha_asset)) = alpha_asset.outcome else {
        panic!("alpha asset read failed");
    };
    let Some(read_asset_response::Outcome::Result(beta_asset)) = beta_asset.outcome else {
        panic!("beta asset read failed");
    };
    assert_ne!(alpha_asset.content, beta_asset.content);

    alpha.shutdown().await;
    beta.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hanging_faults_cancel_with_terminal_events() {
    for fault in ["read.hang-before-stream", "read.hang-after-heartbeat"] {
        let mut fixture =
            FixtureProcess::launch(&format!("cancel-{}", fault.replace('.', "-"))).await;
        fixture
            .open(ADVERSARIAL, "v1", fault, "alpha", "fixture-secret")
            .await
            .unwrap();
        let operation_id = fault.as_bytes();
        let mut stream = fixture
            .catalog_stream(
                operation_id,
                "source.adversarial.catalog",
                ReadMode::Full,
                Vec::new(),
            )
            .await
            .unwrap();
        if fault == "read.hang-after-heartbeat" {
            let heartbeat = timeout(RPC_TIMEOUT, stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(
                heartbeat.event,
                Some(read_catalog_response::Event::Heartbeat(_))
            ));
        }
        let request = fixture.signed(trakkin_provider_sdk::v1::CancelOperationRequest {
            operation_id: operation_id.to_vec(),
        });
        fixture.client.cancel_operation(request).await.unwrap();
        let terminal = timeout(RPC_TIMEOUT, stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            terminal.event,
            Some(read_catalog_response::Event::Cancelled(_))
        ));
        fixture.shutdown().await;
    }
}

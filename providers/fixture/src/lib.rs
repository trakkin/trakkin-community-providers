pub mod scenarios;

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};

use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use trakkin_provider_sdk::{
    negotiate_protocol,
    v1::{
        AccountSnapshot, AuthenticationMethod, AuthenticationProgress, AuthenticationPrompt,
        AuthenticationStatus, CancelAuthenticationRequest, CancelAuthenticationResponse,
        CancelAuthenticationResult, CancelOperationRequest, CancelOperationResponse,
        CancelOperationResult, ConfigurationField, ConfigurationOption, ConfigurationValue,
        ConfigurationValueKind, ContinueAuthenticationRequest, ContinueAuthenticationResponse,
        DescribeConnectionRequest, DescribeConnectionResponse, DescribeConnectionResult,
        DiscoverSourcesRequest, DiscoverSourcesResponse, DiscoverSourcesResult, FieldProblem,
        HandshakeRequest, HandshakeResponse, HealthRequest, HealthResponse, HealthStatus,
        ListAuthenticationMethodsRequest, ListAuthenticationMethodsResponse,
        ListAuthenticationMethodsResult, LookupPortableReferencesRequest,
        LookupPortableReferencesResponse, OpenConnectionRequest, OpenConnectionResponse,
        OpenConnectionResult, OperationFailure, OperationFailureCategory, ReadAssetRequest,
        ReadAssetResponse, ReadCancelled, ReadCatalogRequest, ReadCatalogResponse, ReadHeartbeat,
        ReadStateRequest, ReadStateResponse, ReadTargetedStateRequest, ReadTargetedStateResponse,
        ResolvePortableEndpointsRequest, ResolvePortableEndpointsResponse, RetryAdvice,
        RetryDisposition, ShutdownRequest, ShutdownResponse, StartAuthenticationRequest,
        StartAuthenticationResponse, ValidateConnectionRequest, ValidateConnectionResponse,
        ValidateConnectionResult, Value, WriteTargetedStateRequest, WriteTargetedStateResponse,
        adapter_service_server::AdapterService, cancel_authentication_response,
        cancel_operation_response, continue_authentication_response, describe_connection_response,
        discover_sources_response, list_authentication_methods_response, open_connection_response,
        read_catalog_response, read_state_response, start_authentication_response,
        validate_connection_response, value,
    },
    validation,
};

use crate::scenarios::{FAULT_IDS, FixtureSettings, SCENARIO_IDS, Scenario, ScenarioError};

type CatalogStream = Pin<Box<dyn Stream<Item = Result<ReadCatalogResponse, Status>> + Send>>;
type StateStream = Pin<Box<dyn Stream<Item = Result<ReadStateResponse, Status>> + Send>>;

#[derive(Clone)]
pub struct FixtureAdapter {
    process_instance_id: Arc<str>,
    scenario: Arc<RwLock<Option<Scenario>>>,
    operations: Arc<Mutex<HashMap<Vec<u8>, CancellationToken>>>,
    shutdown: CancellationToken,
}

impl FixtureAdapter {
    pub fn new(process_instance_id: impl Into<Arc<str>>, shutdown: CancellationToken) -> Self {
        Self {
            process_instance_id: process_instance_id.into(),
            scenario: Arc::new(RwLock::new(None)),
            operations: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
        }
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    async fn scenario(&self) -> Result<Scenario, Status> {
        self.scenario
            .read()
            .await
            .clone()
            .ok_or_else(|| Status::failed_precondition("connection is not open"))
    }

    async fn register_operation(&self, operation_id: &[u8]) -> Result<CancellationToken, Status> {
        let token = CancellationToken::new();
        let mut operations = self.operations.lock().await;
        if operations.contains_key(operation_id) {
            return Err(Status::already_exists("operation ID is already active"));
        }
        operations.insert(operation_id.to_vec(), token.clone());
        Ok(token)
    }

    fn configuration_fields() -> Vec<ConfigurationField> {
        vec![
            text_field("scenario", "Scenario", true, SCENARIO_IDS),
            text_field("revision", "Revision", true, ["v1", "v2"]),
            text_field("fault", "Fault", true, FAULT_IDS),
            text_field("instance", "Instance", true, ["alpha", "beta"]),
            ConfigurationField {
                key: "fixture_token".to_owned(),
                display_name: "Fixture token".to_owned(),
                value_kind: ConfigurationValueKind::Text as i32,
                required: true,
                secret: true,
                placeholder: "fixture-secret[-alpha|-beta]".to_owned(),
                ..ConfigurationField::default()
            },
        ]
    }
}

#[tonic::async_trait]
impl AdapterService for FixtureAdapter {
    type ReadCatalogStream = CatalogStream;
    type ReadStateStream = StateStream;

    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        let request = request.into_inner();
        if request.process_instance_id != self.process_instance_id.as_ref() {
            return Err(Status::failed_precondition(
                "process instance ID does not match bootstrap",
            ));
        }
        let selected_protocol = negotiate_protocol(
            request
                .supported_protocol
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("supported protocol is required"))?,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(HandshakeResponse {
            selected_protocol: Some(selected_protocol),
            adapter_key: "dev.trakkin.fixture".to_owned(),
            adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
            process_instance_id: self.process_instance_id.to_string(),
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let response = HealthResponse {
            status: HealthStatus::Ready as i32,
            error: None,
        };
        validation::health_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn describe_connection(
        &self,
        _request: Request<DescribeConnectionRequest>,
    ) -> Result<Response<DescribeConnectionResponse>, Status> {
        let response = DescribeConnectionResponse {
            outcome: Some(describe_connection_response::Outcome::Result(
                DescribeConnectionResult {
                    fields: Self::configuration_fields(),
                },
            )),
        };
        validation::describe_connection_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn validate_connection(
        &self,
        request: Request<ValidateConnectionRequest>,
    ) -> Result<Response<ValidateConnectionResponse>, Status> {
        let request = request.into_inner();
        let field_problems = settings_from(&request.settings, &request.secrets)
            .err()
            .unwrap_or_default();
        let response = ValidateConnectionResponse {
            outcome: Some(validate_connection_response::Outcome::Result(
                ValidateConnectionResult { field_problems },
            )),
        };
        validation::validate_connection_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn list_authentication_methods(
        &self,
        _request: Request<ListAuthenticationMethodsRequest>,
    ) -> Result<Response<ListAuthenticationMethodsResponse>, Status> {
        let response = ListAuthenticationMethodsResponse {
            outcome: Some(list_authentication_methods_response::Outcome::Result(
                ListAuthenticationMethodsResult {
                    methods: ["device", "wait", "deny", "expire", "cancel"]
                        .into_iter()
                        .map(|name| AuthenticationMethod {
                            key: format!("fixture/{name}"),
                            display_name: name.to_owned(),
                            interactions: Vec::new(),
                            fields: Vec::new(),
                        })
                        .collect(),
                },
            )),
        };
        validation::list_authentication_methods_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn start_authentication(
        &self,
        request: Request<StartAuthenticationRequest>,
    ) -> Result<Response<StartAuthenticationResponse>, Status> {
        let method = request.into_inner().method;
        let step = match method.as_str() {
            "fixture/device" => StartAuthenticationResponse {
                outcome: Some(start_authentication_response::Outcome::Result(
                    AuthenticationProgress {
                        authentication_id: "auth-device".to_owned(),
                        status: AuthenticationStatus::InputRequired as i32,
                        prompt: Some(AuthenticationPrompt {
                            message: "Enter the fixture authentication code".to_owned(),
                            verification_url: "https://example.invalid/fixture".to_owned(),
                            user_code: "FIXTURE".to_owned(),
                            fields: vec![text_field("code", "Code", true, ["approved"])],
                        }),
                        ..AuthenticationProgress::default()
                    },
                )),
            },
            "fixture/wait" | "fixture/cancel" => StartAuthenticationResponse {
                outcome: Some(start_authentication_response::Outcome::Result(
                    AuthenticationProgress {
                        authentication_id: method.replace("fixture/", "auth-"),
                        status: AuthenticationStatus::Waiting as i32,
                        retry_after: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        }),
                        ..AuthenticationProgress::default()
                    },
                )),
            },
            "fixture/deny" => failed_start_auth("authentication_denied"),
            "fixture/expire" => StartAuthenticationResponse {
                outcome: Some(start_authentication_response::Outcome::Result(
                    AuthenticationProgress {
                        authentication_id: "auth-expire".to_owned(),
                        status: AuthenticationStatus::Expired as i32,
                        ..AuthenticationProgress::default()
                    },
                )),
            },
            _ => failed_start_auth("authentication_method_unknown"),
        };
        validation::start_authentication_response(&step)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(step))
    }

    async fn continue_authentication(
        &self,
        request: Request<ContinueAuthenticationRequest>,
    ) -> Result<Response<ContinueAuthenticationResponse>, Status> {
        let request = request.into_inner();
        let approved = request.values.iter().any(|entry| {
            entry.key == "code" && text_value(entry.value.as_ref()) == Some("approved")
        });
        let step = match request.authentication_id.as_str() {
            "auth-device" if approved => ContinueAuthenticationResponse {
                outcome: Some(continue_authentication_response::Outcome::Result(
                    AuthenticationProgress {
                        authentication_id: request.authentication_id,
                        status: AuthenticationStatus::Completed as i32,
                        accounts: vec![AccountSnapshot {
                            key: Some(key("account.authenticated")),
                            display_name: "Authenticated fixture account".to_owned(),
                        }],
                        ..AuthenticationProgress::default()
                    },
                )),
            },
            "auth-wait" => ContinueAuthenticationResponse {
                outcome: Some(continue_authentication_response::Outcome::Result(
                    AuthenticationProgress {
                        authentication_id: request.authentication_id,
                        status: AuthenticationStatus::Waiting as i32,
                        retry_after: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        }),
                        ..AuthenticationProgress::default()
                    },
                )),
            },
            _ => failed_continue_auth("authentication_denied"),
        };
        validation::continue_authentication_response(&step)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(step))
    }

    async fn cancel_authentication(
        &self,
        request: Request<CancelAuthenticationRequest>,
    ) -> Result<Response<CancelAuthenticationResponse>, Status> {
        let authentication_id = request.into_inner().authentication_id;
        let outcome = if authentication_id == "auth-cancel" {
            cancel_authentication_response::Outcome::Result(CancelAuthenticationResult {})
        } else {
            cancel_authentication_response::Outcome::Error(adapter_error(
                "authentication_not_cancellable",
                false,
            ))
        };
        let response = CancelAuthenticationResponse {
            outcome: Some(outcome),
        };
        validation::cancel_authentication_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn open_connection(
        &self,
        request: Request<OpenConnectionRequest>,
    ) -> Result<Response<OpenConnectionResponse>, Status> {
        let request = request.into_inner();
        let settings = match settings_from(&request.settings, &request.secrets) {
            Ok(settings) => settings,
            Err(field_problems) => {
                let response = OpenConnectionResponse {
                    outcome: Some(open_connection_response::Outcome::Error(OperationFailure {
                        field_problems,
                        ..operation_failure(
                            "invalid_connection",
                            "connection settings are invalid",
                            false,
                        )
                    })),
                };
                validation::open_connection_response(&response)
                    .map_err(|error| Status::internal(error.to_string()))?;
                return Ok(Response::new(response));
            }
        };
        let scenario = Scenario::load(settings).map_err(scenario_status)?;
        if scenario.settings().fault == "auth.secret-in-error" {
            let response = OpenConnectionResponse {
                outcome: Some(open_connection_response::Outcome::Error(adapter_error(
                    "authentication_failed",
                    false,
                ))),
            };
            validation::open_connection_response(&response)
                .map_err(|error| Status::internal(error.to_string()))?;
            return Ok(Response::new(response));
        }
        let response = OpenConnectionResponse {
            outcome: Some(open_connection_response::Outcome::Result(
                OpenConnectionResult {
                    accounts: scenario.accounts(),
                    capabilities: Some(scenario.connection_capabilities()),
                    secret_patches: Vec::new(),
                },
            )),
        };
        validation::open_connection_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        *self.scenario.write().await = Some(scenario);
        Ok(Response::new(response))
    }

    async fn discover_sources(
        &self,
        _request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        let scenario = self.scenario().await?;
        let response = DiscoverSourcesResponse {
            outcome: Some(discover_sources_response::Outcome::Result(
                DiscoverSourcesResult {
                    sources: scenario.sources(),
                    secret_patches: Vec::new(),
                },
            )),
        };
        let account_keys = scenario
            .accounts()
            .into_iter()
            .filter_map(|account| account.key)
            .collect::<Vec<_>>();
        validation::discover_sources_response(&account_keys, &response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn read_catalog(
        &self,
        request: Request<ReadCatalogRequest>,
    ) -> Result<Response<Self::ReadCatalogStream>, Status> {
        let request = request.into_inner();
        validation::read_catalog_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let scenario = self.scenario().await?;
        let mode = trakkin_provider_sdk::v1::ReadMode::try_from(request.mode)
            .map_err(|_| Status::invalid_argument("read mode is invalid"))?;
        let events = scenario
            .catalog_events(
                request.source_key.as_ref().expect("validated source key"),
                mode,
                &request.prior_cursor,
            )
            .unwrap_or_else(catalog_failure);
        let stream = self
            .catalog_stream(request.operation_id, scenario, events)
            .await?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_state(
        &self,
        request: Request<ReadStateRequest>,
    ) -> Result<Response<Self::ReadStateStream>, Status> {
        let request = request.into_inner();
        validation::read_state_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let scenario = self.scenario().await?;
        let mode = trakkin_provider_sdk::v1::ReadMode::try_from(request.mode)
            .map_err(|_| Status::invalid_argument("read mode is invalid"))?;
        let events = scenario
            .state_events(
                request.source_key.as_ref().expect("validated source key"),
                mode,
                &request.prior_cursor,
            )
            .unwrap_or_else(state_failure);
        let stream = self
            .state_stream(request.operation_id, scenario, events)
            .await?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_targeted_state(
        &self,
        request: Request<ReadTargetedStateRequest>,
    ) -> Result<Response<ReadTargetedStateResponse>, Status> {
        let request = request.into_inner();
        let scenario = self.scenario().await?;
        let source = scenario
            .sources()
            .into_iter()
            .find(|source| source.key.as_ref() == request.source_key.as_ref())
            .ok_or_else(|| Status::not_found("source is unknown"))?;
        let capability = source
            .capabilities
            .and_then(|capabilities| capabilities.targeted_state_read)
            .unwrap_or_default();
        validation::targeted_state_read_request(&request, &capability)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = scenario.targeted_state(&request);
        validation::read_targeted_state_response(&request, &response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn write_targeted_state(
        &self,
        request: Request<WriteTargetedStateRequest>,
    ) -> Result<Response<WriteTargetedStateResponse>, Status> {
        let request = request.into_inner();
        let mut scenario = self.scenario.write().await;
        let scenario = scenario
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("connection is not open"))?;
        let source = scenario
            .sources()
            .into_iter()
            .find(|source| source.key.as_ref() == request.source_key.as_ref())
            .ok_or_else(|| Status::not_found("source is unknown"))?;
        let capability = source
            .capabilities
            .and_then(|capabilities| capabilities.targeted_state_write)
            .unwrap_or_default();
        validation::targeted_state_write_request(&request, &capability)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut response = scenario.write_targeted_state(&request);
        if scenario.settings().fault == "write.malformed-response" {
            response.certainty =
                trakkin_provider_sdk::v1::TargetedStateWriteCertainty::ConfirmedNotApplied as i32;
            return Ok(Response::new(response));
        }
        validation::targeted_state_write_response(&request, &capability, &response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn lookup_portable_references(
        &self,
        request: Request<LookupPortableReferencesRequest>,
    ) -> Result<Response<LookupPortableReferencesResponse>, Status> {
        let request = request.into_inner();
        if request.operation_id.is_empty() {
            return Err(Status::invalid_argument("operation ID is required"));
        }
        let scenario = self.scenario().await?;
        let response = scenario.lookup(&request.references);
        validation::lookup_response(&request.references, &response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn resolve_portable_endpoints(
        &self,
        request: Request<ResolvePortableEndpointsRequest>,
    ) -> Result<Response<ResolvePortableEndpointsResponse>, Status> {
        let request = request.into_inner();
        validation::resolve_endpoints_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let scenario = self.scenario().await?;
        let maximum_response_bytes = request.maximum_response_bytes.min(65_536);
        let response = scenario.resolve_endpoints(&request.endpoints);
        validation::resolve_endpoints_response(
            &request.endpoints,
            &response,
            maximum_response_bytes,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn read_asset(
        &self,
        request: Request<ReadAssetRequest>,
    ) -> Result<Response<ReadAssetResponse>, Status> {
        let request = request.into_inner();
        if request.operation_id.is_empty() || request.maximum_bytes == 0 {
            return Err(Status::invalid_argument(
                "operation ID and maximum bytes are required",
            ));
        }
        let scenario = self.scenario().await?;
        let maximum_bytes = request.maximum_bytes.min(1024);
        let response = scenario.asset(
            request
                .source_key
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("source key is required"))?,
            request
                .provider_item_key
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("provider item key is required"))?,
            request
                .asset_key
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("asset key is required"))?,
            maximum_bytes,
        );
        validation::asset_response(&response, maximum_bytes, &["image/jpeg".to_owned()])
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn cancel_operation(
        &self,
        request: Request<CancelOperationRequest>,
    ) -> Result<Response<CancelOperationResponse>, Status> {
        let operation_id = request.into_inner().operation_id;
        let outcome = if let Some(operation) = self.operations.lock().await.get(&operation_id) {
            operation.cancel();
            cancel_operation_response::Outcome::Result(CancelOperationResult {})
        } else {
            cancel_operation_response::Outcome::Error(adapter_error("operation_not_found", false))
        };
        let response = CancelOperationResponse {
            outcome: Some(outcome),
        };
        validation::cancel_operation_response(&response)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        self.shutdown.cancel();
        for operation in self.operations.lock().await.values() {
            operation.cancel();
        }
        Ok(Response::new(ShutdownResponse {}))
    }
}

impl FixtureAdapter {
    async fn catalog_stream(
        &self,
        operation_id: Vec<u8>,
        scenario: Scenario,
        events: Vec<ReadCatalogResponse>,
    ) -> Result<ReceiverStream<Result<ReadCatalogResponse, Status>>, Status> {
        let token = self.register_operation(&operation_id).await?;
        let operations = self.operations.clone();
        let fault = scenario.settings().fault.clone();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut cancelled = false;
            match fault.as_str() {
                "read.hang-before-stream" => {
                    tokio::select! {
                        _ = token.cancelled() => cancelled = true,
                        _ = sender.closed() => {}
                    }
                }
                "read.hang-after-heartbeat" => {
                    if sender
                        .send(Ok(ReadCatalogResponse {
                            event: Some(read_catalog_response::Event::Heartbeat(ReadHeartbeat {
                                operation_id: operation_id.clone(),
                                records_emitted: 0,
                            })),
                        }))
                        .await
                        .is_ok()
                    {
                        tokio::select! {
                            _ = token.cancelled() => cancelled = true,
                            _ = sender.closed() => {}
                        }
                    }
                }
                _ => {
                    for event in events {
                        tokio::select! {
                            _ = token.cancelled() => {
                                cancelled = true;
                                break;
                            }
                            result = sender.send(Ok(event)) => {
                                if result.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if cancelled {
                let _ = sender
                    .send(Ok(ReadCatalogResponse {
                        event: Some(read_catalog_response::Event::Cancelled(ReadCancelled {})),
                    }))
                    .await;
            }
            operations.lock().await.remove(&operation_id);
        });
        Ok(ReceiverStream::new(receiver))
    }

    async fn state_stream(
        &self,
        operation_id: Vec<u8>,
        scenario: Scenario,
        events: Vec<ReadStateResponse>,
    ) -> Result<ReceiverStream<Result<ReadStateResponse, Status>>, Status> {
        let token = self.register_operation(&operation_id).await?;
        let operations = self.operations.clone();
        let fault = scenario.settings().fault.clone();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut cancelled = false;
            match fault.as_str() {
                "read.hang-before-stream" => {
                    tokio::select! {
                        _ = token.cancelled() => cancelled = true,
                        _ = sender.closed() => {}
                    }
                }
                "read.hang-after-heartbeat" => {
                    if sender
                        .send(Ok(ReadStateResponse {
                            event: Some(read_state_response::Event::Heartbeat(ReadHeartbeat {
                                operation_id: operation_id.clone(),
                                records_emitted: 0,
                            })),
                        }))
                        .await
                        .is_ok()
                    {
                        tokio::select! {
                            _ = token.cancelled() => cancelled = true,
                            _ = sender.closed() => {}
                        }
                    }
                }
                _ => {
                    for event in events {
                        tokio::select! {
                            _ = token.cancelled() => {
                                cancelled = true;
                                break;
                            }
                            result = sender.send(Ok(event)) => {
                                if result.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if cancelled {
                let _ = sender
                    .send(Ok(ReadStateResponse {
                        event: Some(read_state_response::Event::Cancelled(ReadCancelled {})),
                    }))
                    .await;
            }
            operations.lock().await.remove(&operation_id);
        });
        Ok(ReceiverStream::new(receiver))
    }
}

fn settings_from(
    settings: &[ConfigurationValue],
    secrets: &[trakkin_provider_sdk::v1::SecretValue],
) -> Result<FixtureSettings, Vec<FieldProblem>> {
    let mut value = FixtureSettings::default();
    let mut problems = Vec::new();
    let mut present = HashSet::new();
    for setting in settings {
        if !present.insert(setting.key.as_str()) {
            problems.push(field_problem(&setting.key, "duplicate_field"));
            continue;
        }
        let Some(text) = text_value(setting.value.as_ref()) else {
            problems.push(field_problem(&setting.key, "invalid_type"));
            continue;
        };
        match setting.key.as_str() {
            "scenario" => value.scenario_id = text.to_owned(),
            "revision" => value.revision = text.to_owned(),
            "fault" => value.fault = text.to_owned(),
            "instance" => value.instance = text.to_owned(),
            _ => problems.push(field_problem(&setting.key, "unknown_field")),
        }
    }
    for required in ["scenario", "revision", "fault", "instance"] {
        if !present.contains(required) {
            problems.push(field_problem(required, "required"));
        }
    }
    let expected_secret = if value.scenario_id == scenarios::DUAL_CONNECTION_ISOLATION {
        format!("fixture-secret-{}", value.instance)
    } else {
        "fixture-secret".to_owned()
    };
    if secrets.len() != 1
        || secrets[0].key != "fixture_token"
        || secrets[0].value != expected_secret.as_bytes()
    {
        problems.push(field_problem("fixture_token", "invalid_secret"));
    }
    if let Err(error) = Scenario::load(value.clone()) {
        let path = match error {
            ScenarioError::UnknownScenario(_) => "scenario",
            ScenarioError::UnknownRevision(_) => "revision",
            ScenarioError::UnknownFault(_) => "fault",
            ScenarioError::UnknownInstance(_) => "instance",
            _ => "scenario",
        };
        problems.push(field_problem(path, &error.to_string()));
    }
    if problems.is_empty() {
        Ok(value)
    } else {
        Err(problems)
    }
}

fn text_field<const N: usize>(
    key: &str,
    display_name: &str,
    required: bool,
    options: [&str; N],
) -> ConfigurationField {
    ConfigurationField {
        key: key.to_owned(),
        display_name: display_name.to_owned(),
        value_kind: ConfigurationValueKind::Text as i32,
        required,
        secret: false,
        options: options
            .into_iter()
            .map(|option| ConfigurationOption {
                value: Some(Value {
                    value: Some(value::Value::Text(option.to_owned())),
                }),
                display_name: option.to_owned(),
            })
            .collect(),
        ..ConfigurationField::default()
    }
}

fn text_value(value: Option<&Value>) -> Option<&str> {
    match value?.value.as_ref()? {
        value::Value::Text(text) => Some(text),
        _ => None,
    }
}

fn failed_start_auth(code: &str) -> StartAuthenticationResponse {
    StartAuthenticationResponse {
        outcome: Some(start_authentication_response::Outcome::Error(
            adapter_error(code, false),
        )),
    }
}

fn failed_continue_auth(code: &str) -> ContinueAuthenticationResponse {
    ContinueAuthenticationResponse {
        outcome: Some(continue_authentication_response::Outcome::Error(
            adapter_error(code, false),
        )),
    }
}

fn adapter_error(code: &str, retryable: bool) -> OperationFailure {
    operation_failure(code, &code.replace('_', " "), retryable)
}

pub(crate) fn operation_failure(
    code: &str,
    safe_message: &str,
    retryable: bool,
) -> OperationFailure {
    let category = if code.contains("invalid") {
        OperationFailureCategory::InvalidInput
    } else if code.contains("auth") {
        OperationFailureCategory::Authentication
    } else if code.contains("rate") {
        OperationFailureCategory::RateLimited
    } else if code.contains("temporary") || code.contains("unavailable") {
        OperationFailureCategory::Unavailable
    } else if code.contains("precondition") || code.contains("conflict") {
        OperationFailureCategory::Conflict
    } else if code.contains("unsupported") {
        OperationFailureCategory::Unsupported
    } else if code.contains("malformed") {
        OperationFailureCategory::InvalidRemoteData
    } else {
        OperationFailureCategory::Internal
    };
    OperationFailure {
        category: category as i32,
        code: code.to_owned(),
        safe_message: safe_message.to_owned(),
        retry: Some(RetryAdvice {
            disposition: if retryable {
                RetryDisposition::Retryable as i32
            } else {
                RetryDisposition::NotRetryable as i32
            },
            after: None,
        }),
        diagnostic_id: format!("fixture:{code}"),
        ..OperationFailure::default()
    }
}

fn field_problem(path: &str, code: &str) -> FieldProblem {
    FieldProblem {
        path: path.to_owned(),
        code: code.to_owned(),
        message: code.replace('_', " "),
    }
}

fn key(value: &str) -> trakkin_provider_sdk::v1::Key {
    trakkin_provider_sdk::v1::Key {
        namespace: "fixture".to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

fn scenario_status(error: ScenarioError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn catalog_failure(error: ScenarioError) -> Vec<ReadCatalogResponse> {
    vec![ReadCatalogResponse {
        event: Some(read_catalog_response::Event::Failed(
            trakkin_provider_sdk::v1::ReadFailed {
                error: Some(adapter_error(&scenario_error_code(&error), false)),
            },
        )),
    }]
}

fn state_failure(error: ScenarioError) -> Vec<ReadStateResponse> {
    vec![ReadStateResponse {
        event: Some(read_state_response::Event::Failed(
            trakkin_provider_sdk::v1::ReadFailed {
                error: Some(adapter_error(&scenario_error_code(&error), false)),
            },
        )),
    }]
}

fn scenario_error_code(error: &ScenarioError) -> String {
    match error {
        ScenarioError::StaleCursor => "stale_cursor",
        ScenarioError::UnsupportedRead => "unsupported_read",
        ScenarioError::UnknownSource => "source_not_found",
        _ => "invalid_fixture_configuration",
    }
    .to_owned()
}

#[cfg(test)]
mod service_tests {
    use tokio_util::sync::CancellationToken;
    use tonic::Request;
    use trakkin_provider_sdk::v1::{
        ConfigurationValue, OpenConnectionRequest, SecretValue, Value,
        adapter_service_server::AdapterService, open_connection_response, value,
    };

    use super::FixtureAdapter;

    #[tokio::test]
    async fn open_connection_requires_valid_fixture_secret() {
        let adapter = FixtureAdapter::new("process-1", CancellationToken::new());
        let response = adapter
            .open_connection(Request::new(OpenConnectionRequest {
                settings: vec![ConfigurationValue {
                    key: "scenario".to_owned(),
                    value: Some(Value {
                        value: Some(value::Value::Text(
                            crate::scenarios::EMPTY_TARGETED_RECEIVER.to_owned(),
                        )),
                    }),
                }],
                secrets: vec![SecretValue {
                    key: "fixture_token".to_owned(),
                    value: b"wrong".to_vec(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        let Some(open_connection_response::Outcome::Error(error)) = response.outcome else {
            panic!("fixture accepted an invalid connection");
        };
        assert_eq!(error.code, "invalid_connection");
    }
}

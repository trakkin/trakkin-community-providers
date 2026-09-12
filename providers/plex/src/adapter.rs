use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use trakkin_provider_sdk::{
    negotiate_protocol,
    v1::{
        AccountSnapshot, AssetCapability, AuthenticationMethod, AuthenticationProgress,
        AuthenticationPrompt, AuthenticationStatus, CancelAuthenticationRequest,
        CancelAuthenticationResponse, CancelAuthenticationResult, CancelOperationRequest,
        CancelOperationResponse, CancelOperationResult, CatalogBatch, ConfigurationField,
        ConfigurationValue, ConfigurationValueKind, ConnectionCapabilities,
        ContinueAuthenticationRequest, ContinueAuthenticationResponse, DescribeConnectionRequest,
        DescribeConnectionResponse, DescribeConnectionResult, DiscoverSourcesRequest,
        DiscoverSourcesResponse, DiscoverSourcesResult, FieldProblem, HandshakeRequest,
        HandshakeResponse, HealthRequest, HealthResponse, HealthStatus,
        ListAuthenticationMethodsRequest, ListAuthenticationMethodsResponse,
        ListAuthenticationMethodsResult, LookupAmbiguous, LookupCandidate, LookupCapability,
        LookupEvidence, LookupMatched, LookupNotFound, LookupPortableReferencesRequest,
        LookupPortableReferencesResponse, LookupPortableReferencesResult, LookupUnsupported,
        OpenConnectionRequest, OpenConnectionResponse, OpenConnectionResult, OperationFailure,
        OperationFailureCategory, PortableEndpointResolution, PortableReference,
        PortableReferenceLookupResult, ReadAssetRequest, ReadAssetResponse, ReadAssetResult,
        ReadCancelled, ReadCapability, ReadCatalogRequest, ReadCatalogResponse, ReadCompleted,
        ReadFailed, ReadMode, ReadStateRequest, ReadStateResponse, ReadTargetedStateRequest,
        ReadTargetedStateResponse, ResolvePortableEndpointsRequest,
        ResolvePortableEndpointsResponse, ResolvePortableEndpointsResult, RetryAdvice,
        RetryDisposition, SecretPatch, ShutdownRequest, ShutdownResponse, SourceAvailability,
        SourceCapabilities, SourceMembership, SourceSnapshot, StartAuthenticationRequest,
        StartAuthenticationResponse, StateBatch, StatePresence, SubjectReference,
        TargetedStateFieldEffectKind, TargetedStateFieldObservation,
        TargetedStateFieldWriteCapability, TargetedStateMembershipEffect,
        TargetedStateReadCapability, TargetedStateReadIndeterminate, TargetedStateReadMatched,
        TargetedStateReadNotFound, TargetedStateReadUnsupported, TargetedStateWriteCapability,
        TargetedStateWriteCertainty, TargetedStateWriteFieldEffect,
        TargetedStateWriteIdempotencyMode, TargetedStateWritePreconditionMode,
        TargetedStateWriteRetryDisposition, TargetedStateWriteStatus, ValidateConnectionRequest,
        ValidateConnectionResponse, ValidateConnectionResult, Value, WriteTargetedStateRequest,
        WriteTargetedStateResponse, adapter_service_server::AdapterService,
        cancel_authentication_response, cancel_operation_response,
        continue_authentication_response, describe_connection_response, discover_sources_response,
        lookup_portable_references_response, open_connection_response,
        portable_endpoint_resolution, portable_reference_lookup_result, read_asset_response,
        read_catalog_response, read_state_response, read_targeted_state_response, secret_patch,
        start_authentication_response, subject_reference, targeted_state_write_intent,
        validate_connection_response, value,
    },
    validation,
};
use uuid::Uuid;

use crate::{
    ADAPTER_KEY, PRODUCT_NAME,
    client::{PlexClient, PlexError},
    mapping,
    model::{
        LibrarySection, LibrarySections, MediaItem, MetadataContainer, Pin as PlexPin, ServerInfo,
    },
};

const PLEX_TV_URL: &str = "https://plex.tv";
const PLEX_LIBRARY_IDENTIFIER: &str = "com.plexapp.plugins.library";
const MAX_PAGE_SIZE: usize = 1_000;
const MAX_ASSET_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LOOKUP_BATCH: usize = 50;
const MAX_TARGETED_BYTES: u64 = 64 * 1024;
const MAX_RECEIPT_BYTES: u64 = 1_024;
const ASSET_CONTENT_TYPES: &[&str] = &["image/jpeg", "image/png", "image/webp", "image/gif"];

type CatalogStream = Pin<Box<dyn Stream<Item = Result<ReadCatalogResponse, Status>> + Send>>;
type StateStream = Pin<Box<dyn Stream<Item = Result<ReadStateResponse, Status>> + Send>>;

#[derive(Clone)]
pub struct PlexAdapter {
    process_instance_id: Arc<str>,
    client_identifier: Arc<str>,
    cloud_base_url: Arc<str>,
    connection: Arc<RwLock<Option<PlexConnection>>>,
    authentications: Arc<Mutex<HashMap<String, PendingAuthentication>>>,
    operations: Arc<Mutex<HashMap<Vec<u8>, CancellationToken>>>,
    shutdown: CancellationToken,
}

#[derive(Clone)]
struct PlexConnection {
    client: PlexClient,
    server: ServerInfo,
    sections: Arc<RwLock<HashMap<String, LibrarySection>>>,
}

#[derive(Clone)]
struct PendingAuthentication {
    pin_id: u64,
    code: String,
    expires_at: Instant,
}

#[derive(Debug)]
struct PlexSettings {
    server_url: String,
    token: String,
}

impl PlexAdapter {
    pub fn new(process_instance_id: impl Into<Arc<str>>, shutdown: CancellationToken) -> Self {
        Self::with_cloud_base_url(process_instance_id, shutdown, PLEX_TV_URL)
    }

    pub fn with_cloud_base_url(
        process_instance_id: impl Into<Arc<str>>,
        shutdown: CancellationToken,
        cloud_base_url: impl Into<Arc<str>>,
    ) -> Self {
        let process_instance_id = process_instance_id.into();
        Self {
            client_identifier: format!("trakkin-{}", process_instance_id).into(),
            process_instance_id,
            cloud_base_url: cloud_base_url.into(),
            connection: Arc::new(RwLock::new(None)),
            authentications: Arc::new(Mutex::new(HashMap::new())),
            operations: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
        }
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    fn configuration_fields() -> Vec<ConfigurationField> {
        vec![
            ConfigurationField {
                key: "server_url".to_owned(),
                display_name: "Plex server URL".to_owned(),
                value_kind: ConfigurationValueKind::Text as i32,
                required: true,
                placeholder: "http://localhost:32400".to_owned(),
                ..ConfigurationField::default()
            },
            ConfigurationField {
                key: "token".to_owned(),
                display_name: "Plex token".to_owned(),
                value_kind: ConfigurationValueKind::Text as i32,
                required: true,
                secret: true,
                ..ConfigurationField::default()
            },
        ]
    }

    async fn connection(&self) -> Result<PlexConnection, Status> {
        self.connection
            .read()
            .await
            .clone()
            .ok_or_else(|| Status::failed_precondition("connection is not open"))
    }

    async fn register_operation(&self, operation_id: &[u8]) -> Result<CancellationToken, Status> {
        if operation_id.is_empty() {
            return Err(Status::invalid_argument("operation ID is required"));
        }
        let token = CancellationToken::new();
        let mut operations = self.operations.lock().await;
        if operations.contains_key(operation_id) {
            return Err(Status::already_exists("operation ID is already active"));
        }
        operations.insert(operation_id.to_vec(), token.clone());
        Ok(token)
    }

    async fn load_sections(connection: &PlexConnection) -> Result<Vec<LibrarySection>, PlexError> {
        let sections = match connection
            .client
            .get_container::<LibrarySections>("/library/sections/all")
            .await
        {
            Ok(sections) => sections,
            Err(error) if error.status() == Some(StatusCode::NOT_FOUND) => {
                connection
                    .client
                    .get_container::<LibrarySections>("/library/sections")
                    .await?
            }
            Err(error) => return Err(error),
        };
        let mut cache = connection.sections.write().await;
        *cache = sections
            .directories
            .iter()
            .cloned()
            .map(|section| (section.key.clone(), section))
            .collect();
        Ok(sections.directories)
    }

    async fn source_section(
        connection: &PlexConnection,
        source_key: Option<&trakkin_provider_sdk::v1::Key>,
    ) -> Result<LibrarySection, Status> {
        let source_key =
            source_key.ok_or_else(|| Status::invalid_argument("source key is required"))?;
        let section_key = mapping::parse_source_key(source_key)
            .ok_or_else(|| Status::invalid_argument("source key is invalid"))?;
        if let Some(section) = connection.sections.read().await.get(section_key).cloned() {
            return Ok(section);
        }
        Self::load_sections(connection)
            .await
            .map_err(plex_status)?
            .into_iter()
            .find(|section| section.key == section_key)
            .ok_or_else(|| Status::not_found("source is unknown"))
    }

    fn source_snapshot(connection: &PlexConnection, section: LibrarySection) -> SourceSnapshot {
        SourceSnapshot {
            key: Some(mapping::source_key(&section.key)),
            account_key: Some(mapping::account_key(&connection.server.machine_identifier)),
            display_name: section.title,
            kind: Some(mapping::term(mapping::MEDIA_NAMESPACE, section.media_type)),
            availability: SourceAvailability::Available as i32,
            capabilities: Some(source_capabilities()),
        }
    }

    async fn catalog_stream(
        &self,
        request: ReadCatalogRequest,
        connection: PlexConnection,
        section: LibrarySection,
    ) -> Result<ReceiverStream<Result<ReadCatalogResponse, Status>>, Status> {
        let token = self.register_operation(&request.operation_id).await?;
        let operations = self.operations.clone();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            run_catalog(request.clone(), connection, section, token, &sender).await;
            operations.lock().await.remove(&request.operation_id);
        });
        Ok(ReceiverStream::new(receiver))
    }

    async fn state_stream(
        &self,
        request: ReadStateRequest,
        connection: PlexConnection,
        section: LibrarySection,
    ) -> Result<ReceiverStream<Result<ReadStateResponse, Status>>, Status> {
        let token = self.register_operation(&request.operation_id).await?;
        let operations = self.operations.clone();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            run_state(request.clone(), connection, section, token, &sender).await;
            operations.lock().await.remove(&request.operation_id);
        });
        Ok(ReceiverStream::new(receiver))
    }
}

#[tonic::async_trait]
impl AdapterService for PlexAdapter {
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
            adapter_key: ADAPTER_KEY.to_owned(),
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
        validation::health_response(&response).map_err(validation_status)?;
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
        validation::describe_connection_response(&response).map_err(validation_status)?;
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
        validation::validate_connection_response(&response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn list_authentication_methods(
        &self,
        _request: Request<ListAuthenticationMethodsRequest>,
    ) -> Result<Response<ListAuthenticationMethodsResponse>, Status> {
        let response = ListAuthenticationMethodsResponse {
            outcome: Some(
                trakkin_provider_sdk::v1::list_authentication_methods_response::Outcome::Result(
                    ListAuthenticationMethodsResult {
                        methods: vec![AuthenticationMethod {
                            key: "plex/pin".to_owned(),
                            display_name: "Sign in with Plex".to_owned(),
                            interactions: Vec::new(),
                            fields: Vec::new(),
                        }],
                    },
                ),
            ),
        };
        validation::list_authentication_methods_response(&response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn start_authentication(
        &self,
        request: Request<StartAuthenticationRequest>,
    ) -> Result<Response<StartAuthenticationResponse>, Status> {
        let request = request.into_inner();
        let outcome = if request.method != "plex/pin" {
            start_authentication_response::Outcome::Error(operation_failure(
                OperationFailureCategory::InvalidInput,
                "authentication_method_unknown",
                "The requested authentication method is not supported.",
                false,
            ))
        } else {
            let cloud = PlexClient::new(
                &self.cloud_base_url,
                None,
                self.client_identifier.to_string(),
            )
            .map_err(plex_status)?;
            let pin = cloud
                .request_json::<PlexPin>(Method::POST, "/api/v2/pins", &[("strong", "true")])
                .await;
            match pin {
                Ok(pin) => {
                    let authentication_id = format!("plex-pin-{}-{}", pin.id, Uuid::new_v4());
                    let expires_at =
                        Instant::now() + Duration::from_secs(pin.expires_in.unwrap_or(300).max(1));
                    self.authentications.lock().await.insert(
                        authentication_id.clone(),
                        PendingAuthentication {
                            pin_id: pin.id,
                            code: pin.code.clone(),
                            expires_at,
                        },
                    );
                    start_authentication_response::Outcome::Result(AuthenticationProgress {
                        authentication_id,
                        status: AuthenticationStatus::InputRequired as i32,
                        prompt: Some(authentication_prompt(&self.client_identifier, &pin.code)),
                        retry_after: Some(prost_types::Duration {
                            seconds: 2,
                            nanos: 0,
                        }),
                        ..AuthenticationProgress::default()
                    })
                }
                Err(error) => start_authentication_response::Outcome::Error(plex_failure(
                    &error,
                    "authentication_start_failed",
                    "Plex authentication could not be started.",
                )),
            }
        };
        let response = StartAuthenticationResponse {
            outcome: Some(outcome),
        };
        validation::start_authentication_response(&response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn continue_authentication(
        &self,
        request: Request<ContinueAuthenticationRequest>,
    ) -> Result<Response<ContinueAuthenticationResponse>, Status> {
        let authentication_id = request.into_inner().authentication_id;
        let pending = self
            .authentications
            .lock()
            .await
            .get(&authentication_id)
            .cloned();
        let outcome = match pending {
            None => continue_authentication_response::Outcome::Error(operation_failure(
                OperationFailureCategory::InvalidInput,
                "authentication_not_found",
                "The Plex authentication attempt was not found.",
                false,
            )),
            Some(pending) if Instant::now() >= pending.expires_at => {
                self.authentications.lock().await.remove(&authentication_id);
                continue_authentication_response::Outcome::Result(AuthenticationProgress {
                    authentication_id,
                    status: AuthenticationStatus::Expired as i32,
                    ..AuthenticationProgress::default()
                })
            }
            Some(pending) => {
                let cloud = PlexClient::new(
                    &self.cloud_base_url,
                    None,
                    self.client_identifier.to_string(),
                )
                .map_err(plex_status)?;
                match cloud
                    .get_json::<PlexPin>(&format!("/api/v2/pins/{}", pending.pin_id))
                    .await
                {
                    Ok(pin) => match pin.auth_token {
                        Some(token) => {
                            self.authentications.lock().await.remove(&authentication_id);
                            continue_authentication_response::Outcome::Result(
                                AuthenticationProgress {
                                    authentication_id,
                                    status: AuthenticationStatus::Completed as i32,
                                    accounts: vec![AccountSnapshot {
                                        key: Some(mapping::key("account:plex")),
                                        display_name: "Plex account".to_owned(),
                                    }],
                                    secret_patches: vec![SecretPatch {
                                        key: "token".to_owned(),
                                        patch: Some(secret_patch::Patch::Set(token.into_bytes())),
                                    }],
                                    ..AuthenticationProgress::default()
                                },
                            )
                        }
                        None => continue_authentication_response::Outcome::Result(
                            AuthenticationProgress {
                                authentication_id,
                                status: AuthenticationStatus::Waiting as i32,
                                prompt: Some(authentication_prompt(
                                    &self.client_identifier,
                                    &pending.code,
                                )),
                                retry_after: Some(prost_types::Duration {
                                    seconds: 2,
                                    nanos: 0,
                                }),
                                ..AuthenticationProgress::default()
                            },
                        ),
                    },
                    Err(error) => continue_authentication_response::Outcome::Error(plex_failure(
                        &error,
                        "authentication_check_failed",
                        "Plex authentication status could not be checked.",
                    )),
                }
            }
        };
        let response = ContinueAuthenticationResponse {
            outcome: Some(outcome),
        };
        validation::continue_authentication_response(&response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn cancel_authentication(
        &self,
        request: Request<CancelAuthenticationRequest>,
    ) -> Result<Response<CancelAuthenticationResponse>, Status> {
        let authentication_id = request.into_inner().authentication_id;
        let outcome = if self
            .authentications
            .lock()
            .await
            .remove(&authentication_id)
            .is_some()
        {
            cancel_authentication_response::Outcome::Result(CancelAuthenticationResult {})
        } else {
            cancel_authentication_response::Outcome::Error(operation_failure(
                OperationFailureCategory::InvalidInput,
                "authentication_not_found",
                "The Plex authentication attempt was not found.",
                false,
            ))
        };
        let response = CancelAuthenticationResponse {
            outcome: Some(outcome),
        };
        validation::cancel_authentication_response(&response).map_err(validation_status)?;
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
                            OperationFailureCategory::InvalidInput,
                            "invalid_connection",
                            "The Plex connection settings are invalid.",
                            false,
                        )
                    })),
                };
                validation::open_connection_response(&response).map_err(validation_status)?;
                return Ok(Response::new(response));
            }
        };
        let client = PlexClient::new(
            &settings.server_url,
            Some(settings.token),
            self.client_identifier.to_string(),
        )
        .map_err(plex_status)?;
        let server = match client.get_container::<ServerInfo>("").await {
            Ok(server) => server,
            Err(error) => {
                let response = OpenConnectionResponse {
                    outcome: Some(open_connection_response::Outcome::Error(plex_failure(
                        &error,
                        "connection_failed",
                        "The Plex server could not be reached or authenticated.",
                    ))),
                };
                validation::open_connection_response(&response).map_err(validation_status)?;
                return Ok(Response::new(response));
            }
        };
        let display_name = if server.friendly_name.is_empty() {
            "Plex Media Server".to_owned()
        } else {
            server.friendly_name.clone()
        };
        let account = AccountSnapshot {
            key: Some(mapping::account_key(&server.machine_identifier)),
            display_name,
        };
        let connection = PlexConnection {
            client,
            server,
            sections: Arc::new(RwLock::new(HashMap::new())),
        };
        *self.connection.write().await = Some(connection);
        let response = OpenConnectionResponse {
            outcome: Some(open_connection_response::Outcome::Result(
                OpenConnectionResult {
                    accounts: vec![account],
                    capabilities: Some(ConnectionCapabilities {
                        reference_lookup: Some(LookupCapability {
                            reference_namespaces: vec![
                                "plex".to_owned(),
                                "imdb".to_owned(),
                                "tmdb".to_owned(),
                                "tvdb".to_owned(),
                            ],
                            maximum_batch_size: MAX_LOOKUP_BATCH as u32,
                        }),
                        endpoint_lookup: None,
                    }),
                    secret_patches: Vec::new(),
                },
            )),
        };
        validation::open_connection_response(&response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn discover_sources(
        &self,
        _request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        let connection = self.connection().await?;
        let outcome = match Self::load_sections(&connection).await {
            Ok(sections) => discover_sources_response::Outcome::Result(DiscoverSourcesResult {
                sources: sections
                    .into_iter()
                    .map(|section| Self::source_snapshot(&connection, section))
                    .collect(),
                secret_patches: Vec::new(),
            }),
            Err(error) => discover_sources_response::Outcome::Error(plex_failure(
                &error,
                "source_discovery_failed",
                "Plex libraries could not be discovered.",
            )),
        };
        let response = DiscoverSourcesResponse {
            outcome: Some(outcome),
        };
        validation::discover_sources_response(
            &[mapping::account_key(&connection.server.machine_identifier)],
            &response,
        )
        .map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn read_catalog(
        &self,
        request: Request<ReadCatalogRequest>,
    ) -> Result<Response<Self::ReadCatalogStream>, Status> {
        let request = request.into_inner();
        validation::read_catalog_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let connection = self.connection().await?;
        let section = Self::source_section(&connection, request.source_key.as_ref()).await?;
        let stream = self.catalog_stream(request, connection, section).await?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_state(
        &self,
        request: Request<ReadStateRequest>,
    ) -> Result<Response<Self::ReadStateStream>, Status> {
        let request = request.into_inner();
        validation::read_state_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let connection = self.connection().await?;
        let section = Self::source_section(&connection, request.source_key.as_ref()).await?;
        let stream = self.state_stream(request, connection, section).await?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_targeted_state(
        &self,
        request: Request<ReadTargetedStateRequest>,
    ) -> Result<Response<ReadTargetedStateResponse>, Status> {
        let request = request.into_inner();
        let capability = targeted_read_capability();
        validation::targeted_state_read_request(&request, &capability)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let connection = self.connection().await?;
        Self::source_section(&connection, request.source_key.as_ref()).await?;
        let Some(rating_key) = subject_rating_key(request.subject.as_ref()) else {
            let response = ReadTargetedStateResponse {
                outcome: Some(read_targeted_state_response::Outcome::Unsupported(
                    TargetedStateReadUnsupported {},
                )),
            };
            validation::read_targeted_state_response(&request, &response)
                .map_err(validation_status)?;
            return Ok(Response::new(response));
        };
        let response = match connection
            .client
            .get_container::<MetadataContainer>(&format!("/library/metadata/{rating_key}"))
            .await
        {
            Ok(container) => match container.metadata.into_iter().next() {
                Some(item) => {
                    let fields = request
                        .fields
                        .iter()
                        .map(|field| TargetedStateFieldObservation {
                            field: Some(field.clone()),
                            presence: if mapping::value_for_field(&item, field).is_some() {
                                StatePresence::Present as i32
                            } else if field == &mapping::rating_field() {
                                StatePresence::Absent as i32
                            } else {
                                StatePresence::Unknown as i32
                            },
                            value: mapping::value_for_field(&item, field),
                        })
                        .collect();
                    ReadTargetedStateResponse {
                        outcome: Some(read_targeted_state_response::Outcome::Matched(
                            TargetedStateReadMatched {
                                membership: SourceMembership::Present as i32,
                                fields,
                                provider_revision: mapping::provider_revision(&item),
                                observed_time_milliseconds: mapping::now_milliseconds(),
                                expires_time_milliseconds: None,
                                precondition: Vec::new(),
                                write_causation: None,
                            },
                        )),
                    }
                }
                None => ReadTargetedStateResponse {
                    outcome: Some(read_targeted_state_response::Outcome::NotFound(
                        TargetedStateReadNotFound {},
                    )),
                },
            },
            Err(error) if error.status() == Some(StatusCode::NOT_FOUND) => {
                ReadTargetedStateResponse {
                    outcome: Some(read_targeted_state_response::Outcome::NotFound(
                        TargetedStateReadNotFound {},
                    )),
                }
            }
            Err(error) => ReadTargetedStateResponse {
                outcome: Some(read_targeted_state_response::Outcome::Indeterminate(
                    TargetedStateReadIndeterminate {
                        error: Some(plex_failure(
                            &error,
                            "targeted_state_read_failed",
                            "Plex watch state could not be read.",
                        )),
                    },
                )),
            },
        };
        validation::read_targeted_state_response(&request, &response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn write_targeted_state(
        &self,
        request: Request<WriteTargetedStateRequest>,
    ) -> Result<Response<WriteTargetedStateResponse>, Status> {
        let request = request.into_inner();
        let capability = targeted_write_capability();
        validation::targeted_state_write_request(&request, &capability)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let connection = self.connection().await?;
        Self::source_section(&connection, request.source_key.as_ref()).await?;
        let rating_key = subject_rating_key(request.subject.as_ref())
            .ok_or_else(|| Status::invalid_argument("only Plex provider items are writable"))?;
        let write_result = apply_state_writes(&connection.client, rating_key, &request).await;
        let response = match write_result {
            Ok(()) => {
                let mut hasher = Sha256::new();
                hasher.update(&request.idempotency_key);
                hasher.update(rating_key.as_bytes());
                let digest = hasher.finalize().to_vec();
                let receipt_length =
                    request.maximum_receipt_bytes.min(digest.len() as u64) as usize;
                WriteTargetedStateResponse {
                    status: TargetedStateWriteStatus::Applied as i32,
                    certainty: TargetedStateWriteCertainty::ConfirmedApplied as i32,
                    retry_disposition: TargetedStateWriteRetryDisposition::NotRetryable as i32,
                    membership_effect: TargetedStateMembershipEffect::Unchanged as i32,
                    field_effects: request
                        .intents
                        .iter()
                        .map(|intent| TargetedStateWriteFieldEffect {
                            field: intent.field.clone(),
                            effect: match intent.operation {
                                Some(targeted_state_write_intent::Operation::Set(_)) => {
                                    TargetedStateFieldEffectKind::Set as i32
                                }
                                Some(targeted_state_write_intent::Operation::Clear(_)) => {
                                    TargetedStateFieldEffectKind::Cleared as i32
                                }
                                None => TargetedStateFieldEffectKind::Unknown as i32,
                            },
                            value: match &intent.operation {
                                Some(targeted_state_write_intent::Operation::Set(value)) => {
                                    Some(value.clone())
                                }
                                _ => None,
                            },
                        })
                        .collect(),
                    provider_revision: digest.clone(),
                    successor_precondition: Vec::new(),
                    receipt: digest[..receipt_length].to_vec(),
                    error: None,
                }
            }
            Err(error) => WriteTargetedStateResponse {
                status: TargetedStateWriteStatus::Indeterminate as i32,
                certainty: TargetedStateWriteCertainty::Unknown as i32,
                retry_disposition: TargetedStateWriteRetryDisposition::ReconcileFirst as i32,
                membership_effect: TargetedStateMembershipEffect::Unknown as i32,
                field_effects: request
                    .intents
                    .iter()
                    .map(|intent| TargetedStateWriteFieldEffect {
                        field: intent.field.clone(),
                        effect: TargetedStateFieldEffectKind::Unknown as i32,
                        value: None,
                    })
                    .collect(),
                error: Some(plex_failure(
                    &error,
                    "targeted_state_write_indeterminate",
                    "Plex watch state may have been partially updated.",
                )),
                ..WriteTargetedStateResponse::default()
            },
        };
        validation::targeted_state_write_response(&request, &capability, &response)
            .map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn lookup_portable_references(
        &self,
        request: Request<LookupPortableReferencesRequest>,
    ) -> Result<Response<LookupPortableReferencesResponse>, Status> {
        let request = request.into_inner();
        if request.operation_id.is_empty()
            || request.references.is_empty()
            || request.references.len() > MAX_LOOKUP_BATCH
        {
            return Err(Status::invalid_argument(
                "operation ID and between one and 50 references are required",
            ));
        }
        let connection = self.connection().await?;
        let supported = ["plex", "imdb", "tmdb", "tvdb"];
        let mut results = Vec::with_capacity(request.references.len());
        for reference in &request.references {
            let outcome = if !supported.contains(&reference.namespace.as_str()) {
                portable_reference_lookup_result::Outcome::Unsupported(LookupUnsupported {})
            } else if let Ok(value) = std::str::from_utf8(&reference.value) {
                let guid = format!("{}://{value}", reference.namespace);
                let query = [("guid", guid.as_str()), ("includeGuids", "1")];
                match connection
                    .client
                    .get_container_with_query::<MetadataContainer>("/library/all", &query)
                    .await
                {
                    Ok(container) => lookup_outcome(reference, container.metadata),
                    Err(error) => {
                        let response = LookupPortableReferencesResponse {
                            outcome: Some(lookup_portable_references_response::Outcome::Error(
                                plex_failure(
                                    &error,
                                    "reference_lookup_failed",
                                    "Plex could not look up the requested references.",
                                ),
                            )),
                        };
                        validation::lookup_response(&request.references, &response)
                            .map_err(validation_status)?;
                        return Ok(Response::new(response));
                    }
                }
            } else {
                portable_reference_lookup_result::Outcome::Unsupported(LookupUnsupported {})
            };
            results.push(PortableReferenceLookupResult {
                requested: Some(reference.clone()),
                outcome: Some(outcome),
            });
        }
        let response = LookupPortableReferencesResponse {
            outcome: Some(lookup_portable_references_response::Outcome::Result(
                LookupPortableReferencesResult { results },
            )),
        };
        validation::lookup_response(&request.references, &response).map_err(validation_status)?;
        Ok(Response::new(response))
    }

    async fn resolve_portable_endpoints(
        &self,
        request: Request<ResolvePortableEndpointsRequest>,
    ) -> Result<Response<ResolvePortableEndpointsResponse>, Status> {
        let request = request.into_inner();
        validation::resolve_endpoints_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = ResolvePortableEndpointsResponse {
            outcome: Some(
                trakkin_provider_sdk::v1::resolve_portable_endpoints_response::Outcome::Result(
                    ResolvePortableEndpointsResult {
                        results: request
                            .endpoints
                            .iter()
                            .map(|endpoint| PortableEndpointResolution {
                                requested: Some(endpoint.clone()),
                                outcome: Some(portable_endpoint_resolution::Outcome::Unsupported(
                                    LookupUnsupported {},
                                )),
                            })
                            .collect(),
                    },
                ),
            ),
        };
        validation::resolve_endpoints_response(
            &request.endpoints,
            &response,
            request.maximum_response_bytes,
        )
        .map_err(validation_status)?;
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
        let connection = self.connection().await?;
        Self::source_section(&connection, request.source_key.as_ref()).await?;
        let provider_item_key = request
            .provider_item_key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("provider item key is required"))?;
        let rating_key = mapping::parse_item_key(provider_item_key)
            .ok_or_else(|| Status::invalid_argument("provider item key is invalid"))?;
        let asset_key = request
            .asset_key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("asset key is required"))?;
        let asset_kind = mapping::asset_kind(asset_key)
            .ok_or_else(|| Status::invalid_argument("asset key is invalid"))?;
        let maximum_bytes = request.maximum_bytes.min(MAX_ASSET_BYTES);
        let asset = connection
            .client
            .get_container::<MetadataContainer>(&format!("/library/metadata/{rating_key}"))
            .await
            .ok()
            .and_then(|container| container.metadata.into_iter().next())
            .and_then(|item| match asset_kind {
                "poster" => item.thumb,
                "backdrop" => item.art,
                _ => None,
            });
        let response = match asset {
            Some(path) => connection.client.get_bytes(&path, maximum_bytes).await,
            None => Err(PlexError::AssetNotFound),
        };
        let response = match response {
            Ok((content, content_type)) => {
                let digest = Sha256::digest(&content).to_vec();
                ReadAssetResponse {
                    outcome: Some(read_asset_response::Outcome::Result(ReadAssetResult {
                        full_length: content.len() as u64,
                        content,
                        content_type,
                        hash: Some(trakkin_provider_sdk::v1::ContentHash {
                            algorithm: Some(mapping::term("trakkin", "sha256")),
                            digest,
                        }),
                        cache_control: "private, max-age=86400".to_owned(),
                    })),
                }
            }
            Err(error) => ReadAssetResponse {
                outcome: Some(read_asset_response::Outcome::Error(plex_failure(
                    &error,
                    "asset_read_failed",
                    "The Plex artwork could not be read.",
                ))),
            },
        };
        validation::asset_response(
            &response,
            maximum_bytes,
            &ASSET_CONTENT_TYPES
                .iter()
                .map(|content_type| (*content_type).to_owned())
                .collect::<Vec<_>>(),
        )
        .map_err(validation_status)?;
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
            cancel_operation_response::Outcome::Error(operation_failure(
                OperationFailureCategory::InvalidInput,
                "operation_not_found",
                "The Plex operation was not found.",
                false,
            ))
        };
        let response = CancelOperationResponse {
            outcome: Some(outcome),
        };
        validation::cancel_operation_response(&response).map_err(validation_status)?;
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

async fn run_catalog(
    request: ReadCatalogRequest,
    connection: PlexConnection,
    section: LibrarySection,
    cancellation: CancellationToken,
    sender: &mpsc::Sender<Result<ReadCatalogResponse, Status>>,
) {
    if ReadMode::try_from(request.mode) != Ok(ReadMode::Full) {
        send_catalog_failed(
            sender,
            unsupported_failure("incremental_catalog_unsupported"),
        )
        .await;
        return;
    }
    let page_size = (request.preferred_batch_size as usize).clamp(1, MAX_PAGE_SIZE);
    let mut sequence = 0;
    for (path, media_type) in catalog_paths(&section) {
        let mut start = 0;
        loop {
            let mut query = vec![("includeGuids", "1")];
            if let Some(media_type) = media_type {
                query.push(("type", media_type));
            }
            let page = tokio::select! {
                _ = cancellation.cancelled() => {
                    send_catalog_cancelled(sender).await;
                    return;
                }
                page = connection.client.get_container_page::<MetadataContainer>(
                    &path,
                    &query,
                    start,
                    page_size,
                ) => page,
            };
            let page = match page {
                Ok(page) => page,
                Err(error) => {
                    send_catalog_failed(
                        sender,
                        plex_failure(
                            &error,
                            "catalog_read_failed",
                            "The Plex library catalog could not be read.",
                        ),
                    )
                    .await;
                    return;
                }
            };
            let returned = page.metadata.len();
            if returned == 0 {
                break;
            }
            let item_upserts = page.metadata.iter().map(mapping::provider_item).collect();
            let relation_upserts = page
                .metadata
                .iter()
                .enumerate()
                .map(|(position, item)| mapping::catalog_relation(item, start + position))
                .collect();
            let event = ReadCatalogResponse {
                event: Some(read_catalog_response::Event::Batch(CatalogBatch {
                    sequence,
                    item_upserts,
                    relation_upserts,
                    ..CatalogBatch::default()
                })),
            };
            if send_catalog_event(sender, &cancellation, event)
                .await
                .is_err()
            {
                return;
            }
            sequence += 1;
            let next = page.offset.saturating_add(returned);
            if page.total_size.is_some_and(|total| next >= total)
                || next <= start
                || (page.total_size.is_none() && returned < page_size)
            {
                break;
            }
            start = next;
        }
    }
    let revision = source_revision(&connection, &section);
    let _ = sender
        .send(Ok(ReadCatalogResponse {
            event: Some(read_catalog_response::Event::Completed(ReadCompleted {
                next_cursor: revision.clone(),
                evidence_revision: revision,
                observed_time_milliseconds: mapping::now_milliseconds(),
            })),
        }))
        .await;
}

async fn run_state(
    request: ReadStateRequest,
    connection: PlexConnection,
    section: LibrarySection,
    cancellation: CancellationToken,
    sender: &mpsc::Sender<Result<ReadStateResponse, Status>>,
) {
    if ReadMode::try_from(request.mode) != Ok(ReadMode::Full) {
        send_state_failed(sender, unsupported_failure("incremental_state_unsupported")).await;
        return;
    }
    let page_size = (request.preferred_batch_size as usize).clamp(1, MAX_PAGE_SIZE);
    let observed_at = mapping::now_milliseconds();
    let mut sequence = 0;
    for (path, media_type) in catalog_paths(&section) {
        let mut start = 0;
        loop {
            let mut query = vec![("includeGuids", "1")];
            if let Some(media_type) = media_type {
                query.push(("type", media_type));
            }
            let page = tokio::select! {
                _ = cancellation.cancelled() => {
                    send_state_cancelled(sender).await;
                    return;
                }
                page = connection.client.get_container_page::<MetadataContainer>(
                    &path,
                    &query,
                    start,
                    page_size,
                ) => page,
            };
            let page = match page {
                Ok(page) => page,
                Err(error) => {
                    send_state_failed(
                        sender,
                        plex_failure(
                            &error,
                            "state_read_failed",
                            "Plex watch state could not be read.",
                        ),
                    )
                    .await;
                    return;
                }
            };
            let returned = page.metadata.len();
            if returned == 0 {
                break;
            }
            let observations = page
                .metadata
                .iter()
                .flat_map(|item| mapping::state_observations(item, observed_at))
                .collect();
            let event = ReadStateResponse {
                event: Some(read_state_response::Event::Batch(StateBatch {
                    sequence,
                    observations,
                })),
            };
            if send_state_event(sender, &cancellation, event)
                .await
                .is_err()
            {
                return;
            }
            sequence += 1;
            let next = page.offset.saturating_add(returned);
            if page.total_size.is_some_and(|total| next >= total)
                || next <= start
                || (page.total_size.is_none() && returned < page_size)
            {
                break;
            }
            start = next;
        }
    }
    let revision = source_revision(&connection, &section);
    let _ = sender
        .send(Ok(ReadStateResponse {
            event: Some(read_state_response::Event::Completed(ReadCompleted {
                next_cursor: revision.clone(),
                evidence_revision: revision,
                observed_time_milliseconds: observed_at,
            })),
        }))
        .await;
}

async fn send_catalog_event(
    sender: &mpsc::Sender<Result<ReadCatalogResponse, Status>>,
    cancellation: &CancellationToken,
    event: ReadCatalogResponse,
) -> Result<(), ()> {
    tokio::select! {
        _ = cancellation.cancelled() => {
            send_catalog_cancelled(sender).await;
            Err(())
        }
        result = sender.send(Ok(event)) => result.map_err(|_| ()),
    }
}

async fn send_state_event(
    sender: &mpsc::Sender<Result<ReadStateResponse, Status>>,
    cancellation: &CancellationToken,
    event: ReadStateResponse,
) -> Result<(), ()> {
    tokio::select! {
        _ = cancellation.cancelled() => {
            send_state_cancelled(sender).await;
            Err(())
        }
        result = sender.send(Ok(event)) => result.map_err(|_| ()),
    }
}

async fn send_catalog_failed(
    sender: &mpsc::Sender<Result<ReadCatalogResponse, Status>>,
    error: OperationFailure,
) {
    let _ = sender
        .send(Ok(ReadCatalogResponse {
            event: Some(read_catalog_response::Event::Failed(ReadFailed {
                error: Some(error),
            })),
        }))
        .await;
}

async fn send_state_failed(
    sender: &mpsc::Sender<Result<ReadStateResponse, Status>>,
    error: OperationFailure,
) {
    let _ = sender
        .send(Ok(ReadStateResponse {
            event: Some(read_state_response::Event::Failed(ReadFailed {
                error: Some(error),
            })),
        }))
        .await;
}

async fn send_catalog_cancelled(sender: &mpsc::Sender<Result<ReadCatalogResponse, Status>>) {
    let _ = sender
        .send(Ok(ReadCatalogResponse {
            event: Some(read_catalog_response::Event::Cancelled(ReadCancelled {})),
        }))
        .await;
}

async fn send_state_cancelled(sender: &mpsc::Sender<Result<ReadStateResponse, Status>>) {
    let _ = sender
        .send(Ok(ReadStateResponse {
            event: Some(read_state_response::Event::Cancelled(ReadCancelled {})),
        }))
        .await;
}

fn catalog_paths(section: &LibrarySection) -> Vec<(String, Option<&'static str>)> {
    let path = format!("/library/sections/{}/all", section.key);
    match section.media_type.as_str() {
        "show" => ["2", "3", "4"]
            .into_iter()
            .map(|media_type| (path.clone(), Some(media_type)))
            .collect(),
        "artist" => ["8", "9", "10"]
            .into_iter()
            .map(|media_type| (path.clone(), Some(media_type)))
            .collect(),
        _ => vec![(path, None)],
    }
}

fn source_revision(connection: &PlexConnection, section: &LibrarySection) -> Vec<u8> {
    format!(
        "{}:{}:{}:{}",
        connection.server.machine_identifier,
        connection.server.version,
        connection.server.updated_at,
        section.updated_at,
    )
    .into_bytes()
}

fn source_capabilities() -> SourceCapabilities {
    SourceCapabilities {
        catalog: Some(ReadCapability {
            full: true,
            incremental: false,
        }),
        state: Some(ReadCapability {
            full: true,
            incremental: false,
        }),
        state_fields: mapping::state_fields(),
        assets: Some(AssetCapability {
            maximum_bytes: MAX_ASSET_BYTES,
            content_types: ASSET_CONTENT_TYPES
                .iter()
                .map(|content_type| (*content_type).to_owned())
                .collect(),
        }),
        targeted_state_read: Some(targeted_read_capability()),
        targeted_state_write: Some(targeted_write_capability()),
        ..SourceCapabilities::default()
    }
}

fn targeted_read_capability() -> TargetedStateReadCapability {
    TargetedStateReadCapability {
        maximum_fields: 3,
        maximum_response_bytes: MAX_TARGETED_BYTES,
    }
}

fn targeted_write_capability() -> TargetedStateWriteCapability {
    TargetedStateWriteCapability {
        fields: vec![
            TargetedStateFieldWriteCapability {
                field: Some(mapping::watched_field()),
                set_supported: true,
                clear_supported: false,
            },
            TargetedStateFieldWriteCapability {
                field: Some(mapping::progress_field()),
                set_supported: true,
                clear_supported: true,
            },
            TargetedStateFieldWriteCapability {
                field: Some(mapping::rating_field()),
                set_supported: true,
                clear_supported: false,
            },
        ],
        may_create_source_membership: false,
        precondition_mode: TargetedStateWritePreconditionMode::HostRecheckOnly as i32,
        idempotency_mode: TargetedStateWriteIdempotencyMode::StableKey as i32,
        maximum_fields: 3,
        maximum_request_bytes: MAX_TARGETED_BYTES,
        maximum_response_bytes: MAX_TARGETED_BYTES,
        maximum_receipt_bytes: MAX_RECEIPT_BYTES,
    }
}

async fn apply_state_writes(
    client: &PlexClient,
    rating_key: &str,
    request: &WriteTargetedStateRequest,
) -> Result<(), PlexError> {
    for intent in &request.intents {
        let field = intent.field.as_ref().expect("validated write field");
        match (
            field,
            intent
                .operation
                .as_ref()
                .expect("validated write operation"),
        ) {
            (field, targeted_state_write_intent::Operation::Set(value))
                if field == &mapping::watched_field() =>
            {
                let watched = boolean(value).expect("validated watched value type");
                let path = if watched {
                    "/:/scrobble"
                } else {
                    "/:/unscrobble"
                };
                client
                    .send_with_query(
                        Method::PUT,
                        path,
                        &[("identifier", PLEX_LIBRARY_IDENTIFIER), ("key", rating_key)],
                    )
                    .await?;
            }
            (field, targeted_state_write_intent::Operation::Set(value))
                if field == &mapping::progress_field() =>
            {
                let time = integer(value)
                    .expect("validated progress value type")
                    .max(0)
                    .to_string();
                let metadata_key = format!("/library/metadata/{rating_key}");
                client
                    .send_with_query(
                        Method::POST,
                        "/:/timeline",
                        &[
                            ("ratingKey", rating_key),
                            ("key", metadata_key.as_str()),
                            ("state", "stopped"),
                            ("time", time.as_str()),
                        ],
                    )
                    .await?;
            }
            (field, targeted_state_write_intent::Operation::Clear(_))
                if field == &mapping::progress_field() =>
            {
                let metadata_key = format!("/library/metadata/{rating_key}");
                client
                    .send_with_query(
                        Method::POST,
                        "/:/timeline",
                        &[
                            ("ratingKey", rating_key),
                            ("key", metadata_key.as_str()),
                            ("state", "stopped"),
                            ("time", "0"),
                        ],
                    )
                    .await?;
            }
            (field, targeted_state_write_intent::Operation::Set(value))
                if field == &mapping::rating_field() =>
            {
                let rating = decimal(value).expect("validated rating value type");
                client
                    .send_with_query(
                        Method::PUT,
                        "/:/rate",
                        &[
                            ("identifier", PLEX_LIBRARY_IDENTIFIER),
                            ("key", rating_key),
                            ("rating", rating),
                        ],
                    )
                    .await?;
            }
            _ => unreachable!("write request was validated against Plex capabilities"),
        }
    }
    Ok(())
}

fn lookup_outcome(
    requested: &PortableReference,
    items: Vec<MediaItem>,
) -> portable_reference_lookup_result::Outcome {
    let observed_at = mapping::now_milliseconds();
    let mut candidates = items
        .into_iter()
        .map(|item| LookupCandidate {
            provider_item: Some(mapping::provider_item(&item)),
            evidence: Some(LookupEvidence {
                adapter_revision: mapping::provider_revision(&item),
                observed_time_milliseconds: observed_at,
                expires_time_milliseconds: None,
                matched_references: vec![requested.clone()],
            }),
        })
        .collect::<Vec<_>>();
    match candidates.len() {
        0 => portable_reference_lookup_result::Outcome::NotFound(LookupNotFound {}),
        1 => portable_reference_lookup_result::Outcome::Matched(LookupMatched {
            candidate: candidates.pop(),
        }),
        _ => portable_reference_lookup_result::Outcome::Ambiguous(LookupAmbiguous { candidates }),
    }
}

fn subject_rating_key(subject: Option<&SubjectReference>) -> Option<&str> {
    match subject?.subject.as_ref()? {
        subject_reference::Subject::ProviderItemKey(key) => mapping::parse_item_key(key),
        subject_reference::Subject::CatalogRelationKey(_) => None,
    }
}

fn settings_from(
    settings: &[ConfigurationValue],
    secrets: &[trakkin_provider_sdk::v1::SecretValue],
) -> Result<PlexSettings, Vec<FieldProblem>> {
    let mut server_url = None;
    let mut token = None;
    let mut problems = Vec::new();
    let mut seen = HashSet::new();
    for setting in settings {
        if !seen.insert(format!("setting:64:{}", setting.key)) {
            problems.push(field_problem(
                &setting.key,
                "duplicate_field",
                "Field is duplicated.",
            ));
            continue;
        }
        match setting.key.as_str() {
            "server_url" => match text(setting.value.as_ref()) {
                Some(value) if PlexClient::new(value, None, "validation").is_ok() => {
                    server_url = Some(value.to_owned());
                }
                Some(_) => problems.push(field_problem(
                    "server_url",
                    "invalid_url",
                    "Enter an HTTP or HTTPS Plex server URL without credentials or a query.",
                )),
                None => problems.push(field_problem(
                    "server_url",
                    "invalid_type",
                    "Plex server URL must be text.",
                )),
            },
            _ => problems.push(field_problem(
                &setting.key,
                "unknown_field",
                "This connection field is not supported.",
            )),
        }
    }
    for secret in secrets {
        if !seen.insert(format!("secret:64:{}", secret.key)) {
            problems.push(field_problem(
                &secret.key,
                "duplicate_field",
                "Field is duplicated.",
            ));
            continue;
        }
        match secret.key.as_str() {
            "token" if !secret.value.is_empty() => match String::from_utf8(secret.value.clone()) {
                Ok(value) => token = Some(value),
                Err(_) => problems.push(field_problem(
                    "token",
                    "invalid_type",
                    "Plex token must be UTF-8 text.",
                )),
            },
            "token" => problems.push(field_problem(
                "token",
                "required",
                "Plex token is required.",
            )),
            _ => problems.push(field_problem(
                &secret.key,
                "unknown_field",
                "This connection secret is not supported.",
            )),
        }
    }
    if server_url.is_none() && !problems.iter().any(|problem| problem.path == "server_url") {
        problems.push(field_problem(
            "server_url",
            "required",
            "Plex server URL is required.",
        ));
    }
    if token.is_none() && !problems.iter().any(|problem| problem.path == "token") {
        problems.push(field_problem(
            "token",
            "required",
            "Plex token is required.",
        ));
    }
    if problems.is_empty() {
        Ok(PlexSettings {
            server_url: server_url.expect("validated server URL"),
            token: token.expect("validated token"),
        })
    } else {
        Err(problems)
    }
}

fn authentication_prompt(client_identifier: &str, code: &str) -> AuthenticationPrompt {
    let fragment = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("clientID", client_identifier)
        .append_pair("code", code)
        .append_pair("context[device][product]", PRODUCT_NAME)
        .finish();
    AuthenticationPrompt {
        message: "Open Plex to authorize this connection.".to_owned(),
        verification_url: format!("https://app.plex.tv/auth#?{fragment}"),
        user_code: code.to_owned(),
        fields: Vec::new(),
    }
}

fn field_problem(path: &str, code: &str, message: &str) -> FieldProblem {
    FieldProblem {
        path: path.to_owned(),
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

fn text(value: Option<&Value>) -> Option<&str> {
    match value?.value.as_ref()? {
        value::Value::Text(text) => Some(text),
        _ => None,
    }
}

fn boolean(value: &Value) -> Option<bool> {
    match value.value.as_ref()? {
        value::Value::Boolean(boolean) => Some(*boolean),
        _ => None,
    }
}

fn integer(value: &Value) -> Option<i64> {
    match value.value.as_ref()? {
        value::Value::Integer(integer) => Some(*integer),
        _ => None,
    }
}

fn decimal(value: &Value) -> Option<&str> {
    match value.value.as_ref()? {
        value::Value::Decimal(decimal) => Some(&decimal.value),
        _ => None,
    }
}

fn unsupported_failure(code: &str) -> OperationFailure {
    operation_failure(
        OperationFailureCategory::Unsupported,
        code,
        "Plex does not support incremental reads for this source.",
        false,
    )
}

fn plex_failure(error: &PlexError, code: &str, safe_message: &str) -> OperationFailure {
    let (category, retryable) = match error.status() {
        Some(StatusCode::UNAUTHORIZED) => (OperationFailureCategory::Authentication, false),
        Some(StatusCode::FORBIDDEN) => (OperationFailureCategory::Authorization, false),
        Some(StatusCode::TOO_MANY_REQUESTS) => (OperationFailureCategory::RateLimited, true),
        Some(status) if status.is_server_error() => (OperationFailureCategory::Unavailable, true),
        Some(_) | None if matches!(error, PlexError::InvalidBaseUrl | PlexError::AssetTooLarge) => {
            (OperationFailureCategory::InvalidInput, false)
        }
        Some(_) => (OperationFailureCategory::InvalidRemoteData, false),
        None => (OperationFailureCategory::Unavailable, true),
    };
    operation_failure(category, code, safe_message, retryable)
}

fn operation_failure(
    category: OperationFailureCategory,
    code: &str,
    safe_message: &str,
    retryable: bool,
) -> OperationFailure {
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
        diagnostic_id: format!("plex:{}", Uuid::new_v4()),
        ..OperationFailure::default()
    }
}

fn validation_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn plex_status(error: PlexError) -> Status {
    Status::unavailable(
        plex_failure(&error, "plex_request_failed", "The Plex request failed.").safe_message,
    )
}

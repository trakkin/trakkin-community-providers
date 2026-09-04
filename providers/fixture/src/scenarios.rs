use std::collections::HashMap;

use sha2::{Digest, Sha256};
use trakkin_provider_sdk::v1::{
    AccountSnapshot, AssetCapability, Attribute, BinaryAssetReference, CatalogBatch,
    CatalogRelation, ConfigurationValueKind, ConnectionCapabilities, ContentHash,
    CoordinateBacking, CoordinateBinding, CoordinateCapability, EndpointLookupAmbiguous,
    EndpointLookupCandidate, EndpointLookupCapability, EndpointLookupMatched, Key, LookupAmbiguous,
    LookupCandidate, LookupCapability, LookupEvidence, LookupMatched, LookupNotFound,
    LookupPortableReferencesResponse, LookupPortableReferencesResult, LookupUnsupported,
    OperationFailure, PortableEndpoint, PortableEndpointResolution, PortableReference,
    PortableReferenceLookupResult, ProviderItem, ReadAssetResponse, ReadAssetResult, ReadCancelled,
    ReadCapability, ReadCatalogResponse, ReadCompleted, ReadFailed, ReadMode, ReadStateResponse,
    ReadTargetedStateRequest, ReadTargetedStateResponse, ResolvePortableEndpointsResponse,
    ResolvePortableEndpointsResult, SourceAvailability, SourceCapabilities, SourceMembership,
    SourceSnapshot, StateBatch, StateField, StateFieldDescriptor, StateFieldNumericRange,
    StateFieldQuantizer, StateObservation, StatePresence, SubjectReference,
    TargetedStateFieldEffectKind, TargetedStateFieldObservation, TargetedStateFieldWriteCapability,
    TargetedStateMembershipEffect, TargetedStateReadAmbiguous, TargetedStateReadCapability,
    TargetedStateReadIndeterminate, TargetedStateReadMatched, TargetedStateReadNotFound,
    TargetedStateReadUnsupported, TargetedStateWriteCapability, TargetedStateWriteCausation,
    TargetedStateWriteCertainty, TargetedStateWriteIdempotencyMode,
    TargetedStateWritePreconditionMode, TargetedStateWriteRetryDisposition,
    TargetedStateWriteStatus, Term, Value, WriteTargetedStateRequest, WriteTargetedStateResponse,
    lookup_portable_references_response, portable_endpoint_resolution,
    portable_reference_lookup_result, read_asset_response, read_catalog_response,
    read_state_response, read_targeted_state_response, resolve_portable_endpoints_response,
    state_observation, subject_reference, targeted_state_write_intent, value,
};

use crate::operation_failure;

pub const CATALOG_SCHEMA: &str = "fixtures.catalog/v1";
pub const LOGICAL_TIME_MILLISECONDS: i64 = 1_893_456_245_000;

pub const HIERARCHICAL_CATALOG_ASSETS: &str = "hierarchical-catalog-assets-v1";
pub const EMPTY_TARGETED_RECEIVER: &str = "empty-targeted-receiver-v1";
pub const EDITION_HIERARCHY: &str = "edition-hierarchy-v1";
pub const FLAT_CATALOG_REVISIONS: &str = "flat-catalog-revisions-v1";
pub const ADVERSARIAL: &str = "adversarial-v1";
pub const DUAL_CONNECTION_ISOLATION: &str = "dual-connection-isolation-v1";

pub const SCENARIO_IDS: [&str; 6] = [
    HIERARCHICAL_CATALOG_ASSETS,
    EMPTY_TARGETED_RECEIVER,
    EDITION_HIERARCHY,
    FLAT_CATALOG_REVISIONS,
    ADVERSARIAL,
    DUAL_CONNECTION_ISOLATION,
];

pub const FAULT_IDS: [&str; 12] = [
    "none",
    "read.stale-cursor",
    "read.cancel-after-batch",
    "read.retryable-after-batch",
    "read.fatal-after-batch",
    "read.malformed-catalog",
    "read.malformed-state",
    "read.hang-before-stream",
    "read.hang-after-heartbeat",
    "asset.over-limit",
    "auth.secret-in-error",
    "write.malformed-response",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureSettings {
    pub scenario_id: String,
    pub revision: String,
    pub fault: String,
    pub instance: String,
}

impl Default for FixtureSettings {
    fn default() -> Self {
        Self {
            scenario_id: HIERARCHICAL_CATALOG_ASSETS.to_owned(),
            revision: "v1".to_owned(),
            fault: "none".to_owned(),
            instance: "alpha".to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Scenario {
    settings: FixtureSettings,
    targeted_membership_present: bool,
    targeted_state_values: HashMap<String, Value>,
    targeted_state_revision: u64,
    targeted_write_results: HashMap<Vec<u8>, TargetedWriteRecord>,
}

#[derive(Clone, Debug)]
struct TargetedWriteRecord {
    request: WriteTargetedStateRequest,
    response: WriteTargetedStateResponse,
}

impl Scenario {
    pub fn load(settings: FixtureSettings) -> Result<Self, ScenarioError> {
        if !SCENARIO_IDS.contains(&settings.scenario_id.as_str()) {
            return Err(ScenarioError::UnknownScenario(settings.scenario_id));
        }
        if !matches!(settings.revision.as_str(), "v1" | "v2") {
            return Err(ScenarioError::UnknownRevision(settings.revision));
        }
        if !FAULT_IDS.contains(&settings.fault.as_str()) {
            return Err(ScenarioError::UnknownFault(settings.fault));
        }
        if !matches!(settings.instance.as_str(), "alpha" | "beta") {
            return Err(ScenarioError::UnknownInstance(settings.instance));
        }
        Ok(Self {
            settings,
            targeted_membership_present: false,
            targeted_state_values: HashMap::new(),
            targeted_state_revision: 1,
            targeted_write_results: HashMap::new(),
        })
    }

    pub fn settings(&self) -> &FixtureSettings {
        &self.settings
    }

    pub fn accounts(&self) -> Vec<AccountSnapshot> {
        let account = match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS => "account.assets.primary",
            EMPTY_TARGETED_RECEIVER => "account.receiver.primary",
            EDITION_HIERARCHY => "account.editions.primary",
            FLAT_CATALOG_REVISIONS => "account.flat.primary",
            ADVERSARIAL => "account.adversarial.edge",
            DUAL_CONNECTION_ISOLATION => {
                if self.settings.instance == "beta" {
                    "account.beta"
                } else {
                    "account.alpha"
                }
            }
            _ => unreachable!("scenario was validated"),
        };
        vec![AccountSnapshot {
            key: Some(key(account)),
            display_name: account.to_owned(),
        }]
    }

    pub fn connection_capabilities(&self) -> ConnectionCapabilities {
        let supported = matches!(
            self.settings.scenario_id.as_str(),
            EMPTY_TARGETED_RECEIVER | ADVERSARIAL
        );
        ConnectionCapabilities {
            reference_lookup: supported.then(|| LookupCapability {
                reference_namespaces: vec!["example.media".to_owned()],
                maximum_batch_size: 50,
            }),
            endpoint_lookup: supported.then(|| EndpointLookupCapability {
                reference_namespaces: vec!["example.media".to_owned()],
                coordinate_ids: vec!["segment".to_owned()],
                maximum_batch_size: 50,
                maximum_response_bytes: 65_536,
            }),
        }
    }

    pub fn sources(&self) -> Vec<SourceSnapshot> {
        let account_key = self.accounts()[0].key.clone();
        match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS => vec![
                source(
                    "source.assets.catalog",
                    "Asset Catalog",
                    "media",
                    account_key.clone(),
                    true,
                    true,
                    true,
                ),
                source(
                    "source.hierarchy.catalog",
                    "Hierarchical Catalog",
                    "media",
                    account_key,
                    true,
                    true,
                    false,
                ),
            ],
            EMPTY_TARGETED_RECEIVER => vec![
                source(
                    "source.lookup.catalog",
                    "Lookup Catalog",
                    "media",
                    account_key.clone(),
                    true,
                    true,
                    false,
                ),
                source(
                    "source.reference.catalog",
                    "Reference Catalog",
                    "media",
                    account_key.clone(),
                    true,
                    true,
                    false,
                ),
                source(
                    "source.receiver.empty",
                    "Empty Receiver",
                    "media",
                    account_key,
                    true,
                    true,
                    false,
                ),
            ],
            EDITION_HIERARCHY => vec![source(
                "source.editions.catalog",
                "Edition Catalog",
                "media",
                account_key,
                false,
                true,
                false,
            )],
            FLAT_CATALOG_REVISIONS => vec![source(
                "source.flat.catalog",
                "Flat Catalog",
                "media",
                account_key,
                true,
                true,
                false,
            )],
            ADVERSARIAL => vec![
                source(
                    "source.adversarial.catalog",
                    "Adversarial Catalog",
                    "mixed",
                    account_key.clone(),
                    true,
                    true,
                    true,
                ),
                source(
                    "source.adversarial.state",
                    "Adversarial State",
                    "mixed",
                    account_key.clone(),
                    true,
                    true,
                    false,
                ),
                source(
                    "source.adversarial.lookup",
                    "Adversarial Lookup",
                    "mixed",
                    account_key.clone(),
                    true,
                    false,
                    false,
                ),
                source(
                    "source.adversarial.lookup-unsupported",
                    "Unsupported Lookup",
                    "mixed",
                    account_key,
                    false,
                    false,
                    false,
                ),
            ],
            DUAL_CONNECTION_ISOLATION => {
                let name = format!("source.{}.catalog", self.settings.instance);
                vec![source(&name, &name, "media", account_key, true, true, true)]
            }
            _ => unreachable!("scenario was validated"),
        }
    }

    pub fn catalog_events(
        &self,
        source_key: &Key,
        mode: ReadMode,
        prior_cursor: &[u8],
    ) -> Result<Vec<ReadCatalogResponse>, ScenarioError> {
        self.ensure_source(source_key)?;
        if self.settings.fault == "read.stale-cursor" {
            return Ok(failed_catalog("stale_cursor", false));
        }
        let source = text_key(source_key)?;
        let incremental = self.read_mode(source_key, mode, prior_cursor, "catalog")?;
        let mut batch = if incremental {
            self.incremental_catalog(source)
        } else {
            self.full_catalog(source)
        };
        if self.settings.fault == "read.malformed-catalog" {
            batch.sequence = 9;
        }
        let mut events = vec![ReadCatalogResponse {
            event: Some(read_catalog_response::Event::Batch(batch)),
        }];
        match self.settings.fault.as_str() {
            "read.cancel-after-batch" => events.push(ReadCatalogResponse {
                event: Some(read_catalog_response::Event::Cancelled(ReadCancelled {})),
            }),
            "read.retryable-after-batch" => {
                events.extend(failed_catalog("temporary_failure", true))
            }
            "read.fatal-after-batch" => events.extend(failed_catalog("fatal_failure", false)),
            _ => events.push(ReadCatalogResponse {
                event: Some(read_catalog_response::Event::Completed(
                    self.completed(source, "catalog"),
                )),
            }),
        }
        Ok(events)
    }

    pub fn state_events(
        &self,
        source_key: &Key,
        mode: ReadMode,
        prior_cursor: &[u8],
    ) -> Result<Vec<ReadStateResponse>, ScenarioError> {
        self.ensure_source(source_key)?;
        if self.settings.fault == "read.stale-cursor" {
            return Ok(failed_state("stale_cursor", false));
        }
        let source = text_key(source_key)?;
        let incremental = self.read_mode(source_key, mode, prior_cursor, "state")?;
        let mut batch = if incremental {
            self.incremental_state(source)
        } else {
            self.full_state(source)
        };
        if self.settings.fault == "read.malformed-state" {
            batch.sequence = 9;
        }
        let mut events = vec![ReadStateResponse {
            event: Some(read_state_response::Event::Batch(batch)),
        }];
        match self.settings.fault.as_str() {
            "read.cancel-after-batch" => events.push(ReadStateResponse {
                event: Some(read_state_response::Event::Cancelled(ReadCancelled {})),
            }),
            "read.retryable-after-batch" => events.extend(failed_state("temporary_failure", true)),
            "read.fatal-after-batch" => events.extend(failed_state("fatal_failure", false)),
            _ => events.push(ReadStateResponse {
                event: Some(read_state_response::Event::Completed(
                    self.completed(source, "state"),
                )),
            }),
        }
        Ok(events)
    }

    pub fn lookup(&self, references: &[PortableReference]) -> LookupPortableReferencesResponse {
        let results = references
            .iter()
            .map(|requested| PortableReferenceLookupResult {
                requested: Some(requested.clone()),
                outcome: Some(self.lookup_outcome(requested)),
            })
            .collect();
        LookupPortableReferencesResponse {
            outcome: Some(lookup_portable_references_response::Outcome::Result(
                LookupPortableReferencesResult { results },
            )),
        }
    }

    pub fn resolve_endpoints(
        &self,
        endpoints: &[PortableEndpoint],
    ) -> ResolvePortableEndpointsResponse {
        let results = endpoints
            .iter()
            .map(|requested| PortableEndpointResolution {
                requested: Some(requested.clone()),
                outcome: Some(self.endpoint_lookup_outcome(requested)),
            })
            .collect();
        ResolvePortableEndpointsResponse {
            outcome: Some(resolve_portable_endpoints_response::Outcome::Result(
                ResolvePortableEndpointsResult { results },
            )),
        }
    }

    pub fn targeted_state(&self, request: &ReadTargetedStateRequest) -> ReadTargetedStateResponse {
        let source = request
            .source_key
            .as_ref()
            .and_then(|source| text_key(source).ok());
        let subject = request
            .subject
            .as_ref()
            .and_then(|subject| subject.subject.as_ref())
            .and_then(|subject| match subject {
                subject_reference::Subject::ProviderItemKey(key) => text_key(key).ok(),
                subject_reference::Subject::CatalogRelationKey(_) => None,
            });
        let outcome = match (self.settings.scenario_id.as_str(), source, subject) {
            (
                EMPTY_TARGETED_RECEIVER,
                Some("source.receiver.empty"),
                Some("media.quiet-signal"),
            ) => {
                let provider_revision =
                    format!("targeted.receiver.empty.v{}", self.targeted_state_revision)
                        .into_bytes();
                let write_causation = self
                    .targeted_write_results
                    .get(&request.reconciliation_idempotency_key)
                    .filter(|record| {
                        !request.reconciliation_idempotency_key.is_empty()
                            && record.response.provider_revision == provider_revision
                    })
                    .map(|record| TargetedStateWriteCausation {
                        idempotency_key: request.reconciliation_idempotency_key.clone(),
                        receipt: record.response.receipt.clone(),
                        provider_revision: record.response.provider_revision.clone(),
                    });
                read_targeted_state_response::Outcome::Matched(TargetedStateReadMatched {
                    membership: if self.targeted_membership_present {
                        SourceMembership::Present
                    } else {
                        SourceMembership::Absent
                    } as i32,
                    fields: request
                        .fields
                        .iter()
                        .cloned()
                        .map(|field| {
                            let value = self
                                .targeted_state_values
                                .get(&state_field_identity(&field))
                                .cloned();
                            TargetedStateFieldObservation {
                                field: Some(field),
                                presence: if self.targeted_membership_present {
                                    if value.is_some() {
                                        StatePresence::Present
                                    } else {
                                        StatePresence::Deleted
                                    }
                                } else {
                                    StatePresence::Absent
                                } as i32,
                                value,
                            }
                        })
                        .collect(),
                    provider_revision,
                    observed_time_milliseconds: LOGICAL_TIME_MILLISECONDS
                        + (self.targeted_state_revision as i64 - 1) * 1_000,
                    expires_time_milliseconds: Some(
                        LOGICAL_TIME_MILLISECONDS
                            + (self.targeted_state_revision as i64 - 1) * 1_000
                            + 60_000,
                    ),
                    precondition: if self.targeted_membership_present {
                        format!("expected-present-r{}", self.targeted_state_revision).into_bytes()
                    } else {
                        format!("expected-absent-r{}", self.targeted_state_revision).into_bytes()
                    },
                    write_causation,
                })
            }
            (EMPTY_TARGETED_RECEIVER, Some("source.receiver.empty"), Some("media.linked")) => {
                read_targeted_state_response::Outcome::Ambiguous(TargetedStateReadAmbiguous {
                    candidates: vec![
                        provider_item_subject("media.linked.part1"),
                        provider_item_subject("media.linked.part2"),
                    ],
                })
            }
            (EMPTY_TARGETED_RECEIVER, Some("source.receiver.empty"), Some("missing")) => {
                read_targeted_state_response::Outcome::NotFound(TargetedStateReadNotFound {})
            }
            (EMPTY_TARGETED_RECEIVER, Some("source.receiver.empty"), Some("indeterminate")) => {
                read_targeted_state_response::Outcome::Indeterminate(
                    TargetedStateReadIndeterminate {
                        error: Some(operation_failure(
                            "temporarily_unavailable",
                            "targeted state is temporarily unavailable",
                            true,
                        )),
                    },
                )
            }
            _ => {
                read_targeted_state_response::Outcome::Unsupported(TargetedStateReadUnsupported {})
            }
        };
        ReadTargetedStateResponse {
            outcome: Some(outcome),
        }
    }

    pub fn write_targeted_state(
        &mut self,
        request: &WriteTargetedStateRequest,
    ) -> WriteTargetedStateResponse {
        let mut stable_request = request.clone();
        stable_request.operation_id.clear();
        if let Some(record) = self.targeted_write_results.get(&request.idempotency_key) {
            return if record.request == stable_request {
                record.response.clone()
            } else {
                rejected_targeted_write("idempotency_key_conflict")
            };
        }

        let source = request
            .source_key
            .as_ref()
            .and_then(|source| text_key(source).ok());
        let subject = request
            .subject
            .as_ref()
            .and_then(|subject| subject.subject.as_ref())
            .and_then(|subject| match subject {
                subject_reference::Subject::ProviderItemKey(key) => text_key(key).ok(),
                subject_reference::Subject::CatalogRelationKey(_) => None,
            });
        let expected_membership = if self.targeted_membership_present {
            SourceMembership::Present
        } else {
            SourceMembership::Absent
        };
        let expected_precondition = if self.targeted_membership_present {
            format!("expected-present-r{}", self.targeted_state_revision).into_bytes()
        } else {
            format!("expected-absent-r{}", self.targeted_state_revision).into_bytes()
        };
        if self.settings.scenario_id != EMPTY_TARGETED_RECEIVER
            || source != Some("source.receiver.empty")
            || subject != Some("media.quiet-signal")
            || request.expected_membership != expected_membership as i32
            || request.precondition != expected_precondition
        {
            return rejected_targeted_write("precondition_failed");
        }

        let created_membership = !self.targeted_membership_present;
        self.targeted_membership_present = true;
        let field_effects = request
            .intents
            .iter()
            .map(|intent| {
                let field = intent
                    .field
                    .as_ref()
                    .expect("validated targeted write field");
                let identity = state_field_identity(field);
                let (effect, value) = match intent.operation.as_ref() {
                    Some(targeted_state_write_intent::Operation::Set(value)) => {
                        self.targeted_state_values.insert(identity, value.clone());
                        (TargetedStateFieldEffectKind::Set, Some(value.clone()))
                    }
                    Some(targeted_state_write_intent::Operation::Clear(_)) => {
                        self.targeted_state_values.remove(&identity);
                        (TargetedStateFieldEffectKind::Cleared, None)
                    }
                    None => unreachable!("validated targeted write intent"),
                };
                trakkin_provider_sdk::v1::TargetedStateWriteFieldEffect {
                    field: intent.field.clone(),
                    effect: effect as i32,
                    value,
                }
            })
            .collect();
        self.targeted_state_revision += 1;
        let response = WriteTargetedStateResponse {
            status: TargetedStateWriteStatus::Applied as i32,
            certainty: TargetedStateWriteCertainty::ConfirmedApplied as i32,
            retry_disposition: TargetedStateWriteRetryDisposition::NotRetryable as i32,
            membership_effect: if created_membership {
                TargetedStateMembershipEffect::Created
            } else {
                TargetedStateMembershipEffect::Unchanged
            } as i32,
            field_effects,
            provider_revision: format!("targeted.receiver.empty.v{}", self.targeted_state_revision)
                .into_bytes(),
            successor_precondition: format!("expected-present-r{}", self.targeted_state_revision)
                .into_bytes(),
            receipt: format!("fixture-write-r{}", self.targeted_state_revision).into_bytes(),
            error: None,
        };
        self.targeted_write_results.insert(
            request.idempotency_key.clone(),
            TargetedWriteRecord {
                request: stable_request,
                response: response.clone(),
            },
        );
        response
    }

    pub fn asset(
        &self,
        source_key: &Key,
        provider_item_key: &Key,
        asset_key: &Key,
        maximum_bytes: u64,
    ) -> ReadAssetResponse {
        if self.ensure_source(source_key).is_err() {
            return error_asset("source_not_found", false);
        }
        let binding = (
            text_key(source_key).ok(),
            text_key(provider_item_key).ok(),
            text_key(asset_key).ok(),
        );
        let valid_binding = match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS => {
                binding
                    == (
                        Some("source.assets.catalog"),
                        Some("media.asset.primary"),
                        Some("asset.poster.valid"),
                    )
            }
            ADVERSARIAL => {
                binding
                    == (
                        Some("source.adversarial.catalog"),
                        Some("adversarial.item.left"),
                        Some("asset.adversarial.poster"),
                    )
            }
            DUAL_CONNECTION_ISOLATION => {
                binding
                    == (
                        Some(format!("source.{}.catalog", self.settings.instance).as_str()),
                        Some(format!("media.{}.root", self.settings.instance).as_str()),
                        Some(format!("asset.{}.poster", self.settings.instance).as_str()),
                    )
            }
            _ => false,
        };
        if !valid_binding {
            return error_asset("asset_not_found", false);
        }
        if self.settings.fault == "asset.over-limit" {
            return error_asset("asset_too_large", false);
        }
        let content = match self.settings.scenario_id.as_str() {
            DUAL_CONNECTION_ISOLATION if self.settings.instance == "beta" => {
                b"\xff\xd8\xffFXDUALB\xff\xd9".to_vec()
            }
            DUAL_CONNECTION_ISOLATION => b"\xff\xd8\xffFXDUALA\xff\xd9".to_vec(),
            _ => b"\xff\xd8\xffFXASSET\xff\xd9".to_vec(),
        };
        if content.len() as u64 > maximum_bytes {
            return error_asset("asset_too_large", false);
        }
        ReadAssetResponse {
            outcome: Some(read_asset_response::Outcome::Result(ReadAssetResult {
                full_length: content.len() as u64,
                hash: Some(ContentHash {
                    algorithm: Some(term("sha256")),
                    digest: Sha256::digest(&content).to_vec(),
                }),
                content,
                content_type: "image/jpeg".to_owned(),
                cache_control: "private, max-age=300".to_owned(),
            })),
        }
    }

    fn ensure_source(&self, requested: &Key) -> Result<(), ScenarioError> {
        if self
            .sources()
            .iter()
            .any(|source| source.key.as_ref() == Some(requested))
        {
            Ok(())
        } else {
            Err(ScenarioError::UnknownSource)
        }
    }

    fn read_mode(
        &self,
        source_key: &Key,
        mode: ReadMode,
        prior_cursor: &[u8],
        stream: &str,
    ) -> Result<bool, ScenarioError> {
        let source = self
            .sources()
            .into_iter()
            .find(|source| source.key.as_ref() == Some(source_key))
            .ok_or(ScenarioError::UnknownSource)?;
        let capabilities = source.capabilities.expect("fixture capabilities");
        let read = if stream == "catalog" {
            capabilities.catalog
        } else {
            capabilities.state
        }
        .ok_or(ScenarioError::UnsupportedRead)?;
        match mode {
            ReadMode::Full if read.full && prior_cursor.is_empty() => Ok(false),
            ReadMode::Incremental if read.incremental => {
                let source = text_key(source_key)?;
                let expected = cursor(&self.settings.scenario_id, source, stream, "v1");
                if self.settings.revision != "v2" || prior_cursor != expected {
                    Err(ScenarioError::StaleCursor)
                } else {
                    Ok(true)
                }
            }
            _ => Err(ScenarioError::UnsupportedRead),
        }
    }

    fn completed(&self, source: &str, stream: &str) -> ReadCompleted {
        let revision = self.effective_revision(source, stream);
        ReadCompleted {
            next_cursor: cursor(&self.settings.scenario_id, source, stream, revision),
            evidence_revision: format!(
                "{}/{}/{}/{}",
                self.settings.scenario_id, source, stream, revision
            )
            .into_bytes(),
            observed_time_milliseconds: LOGICAL_TIME_MILLISECONDS
                + if revision == "v2" { 1_000 } else { 0 },
        }
    }

    fn effective_revision(&self, source: &str, stream: &str) -> &str {
        match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source != "source.hierarchy.catalog" => "v1",
            EMPTY_TARGETED_RECEIVER if source != "source.lookup.catalog" || stream != "state" => {
                "v1"
            }
            ADVERSARIAL if stream != "catalog" => "v1",
            _ => &self.settings.revision,
        }
    }

    fn full_catalog(&self, source: &str) -> CatalogBatch {
        let items = match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source == "source.hierarchy.catalog" => {
                let mut values = vec![
                    item(
                        "media.hierarchy.root",
                        "media",
                        "Hierarchy Root",
                        &["media/hierarchy/root"],
                    ),
                    item(
                        "media.hierarchy.group.1",
                        "group",
                        "Group 1",
                        &["media/hierarchy/group/1"],
                    ),
                    item(
                        "media.hierarchy.item.1",
                        "media",
                        if self.settings.revision == "v2" {
                            "Arrival Revised"
                        } else {
                            "Arrival"
                        },
                        &["media/hierarchy/item/1"],
                    ),
                    item(
                        "media.hierarchy.special",
                        "media",
                        "Special",
                        &["media/hierarchy/special"],
                    ),
                ];
                if self.settings.revision == "v2" {
                    values.push(item(
                        "media.hierarchy.item.3",
                        "media",
                        "Signal",
                        &["media/hierarchy/item/3"],
                    ));
                } else {
                    values.push(item(
                        "media.hierarchy.item.2",
                        "media",
                        "Drift",
                        &["media/hierarchy/item/2"],
                    ));
                }
                values
            }
            HIERARCHICAL_CATALOG_ASSETS => vec![asset_item()],
            EMPTY_TARGETED_RECEIVER if source == "source.receiver.empty" => Vec::new(),
            EMPTY_TARGETED_RECEIVER if source == "source.lookup.catalog" => vec![
                item(
                    "media.linked.part1",
                    "media",
                    "Linked Part 1",
                    &["media/linked/part1", "media/linked"],
                ),
                item(
                    "media.linked.part2",
                    "media",
                    "Linked Part 2",
                    &["media/linked/part2", "media/linked"],
                ),
            ],
            EMPTY_TARGETED_RECEIVER => vec![item(
                "media.reference.item",
                "media",
                "Reference Item",
                &["media/reference/item"],
            )],
            EDITION_HIERARCHY => {
                let mut values = vec![
                    item("work.winter-archive", "work", "Winter Archive", &[]),
                    item(
                        "edition.winter.us",
                        "edition",
                        "Winter Archive US",
                        &["isbn/9780000000001"],
                    ),
                    item(
                        "edition.winter.uk",
                        "edition",
                        "Winter Archive UK",
                        &["isbn/9780000000002"],
                    ),
                    item("volume.winter.us.v1", "volume", "Volume 1", &[]),
                    item("volume.winter.us.v2", "volume", "Volume 2", &[]),
                    item("chapter.winter.us.v1.c1", "chapter", "Chapter 1", &[]),
                    item("chapter.winter.us.v1.c2", "chapter", "Chapter 2", &[]),
                ];
                if self.settings.revision == "v2" {
                    values.push(item(
                        "edition.winter.alt",
                        "edition",
                        "Winter Archive Alternate",
                        &["isbn/9780000000003"],
                    ));
                }
                values
            }
            FLAT_CATALOG_REVISIONS => {
                let mut values = vec![
                    item_with_attribute(
                        "media.flat.original",
                        "media",
                        "Shared Title",
                        &["media/flat/original"],
                        "release_date",
                        if self.settings.revision == "v2" {
                            "2029-01-16"
                        } else {
                            "2029-01-15"
                        },
                    ),
                    item_with_attribute(
                        "media.flat.alternate",
                        "media",
                        "Shared Title",
                        &["media/flat/alternate"],
                        "release_date",
                        "2029-03-01",
                    ),
                ];
                if self.settings.revision == "v2" {
                    values.push(item_with_attribute(
                        "media.flat.revision",
                        "media",
                        "Shared Title",
                        &["media/flat/revision"],
                        "release_date",
                        "2030-02-02",
                    ));
                }
                values
            }
            ADVERSARIAL => vec![
                item_with_asset(
                    "adversarial.item.left",
                    "media",
                    "Duplicate Left",
                    &["duplicate"],
                    "asset.adversarial.poster",
                ),
                item(
                    "adversarial.item.right",
                    "media",
                    "Duplicate Right",
                    &["duplicate"],
                ),
                item("adversarial.item.missing", "media", "No Reference", &[]),
            ],
            DUAL_CONNECTION_ISOLATION => {
                isolated_connection_items(&self.settings.instance, self.settings.revision == "v2")
            }
            _ => Vec::new(),
        };
        let relation_upserts = self.full_relations(source, &items);
        let coordinate_binding_upserts = self.coordinate_bindings(source);
        CatalogBatch {
            sequence: 0,
            relation_upserts,
            item_upserts: items,
            coordinate_binding_upserts,
            ..CatalogBatch::default()
        }
    }

    fn coordinate_bindings(&self, source: &str) -> Vec<CoordinateBinding> {
        match (self.settings.scenario_id.as_str(), source) {
            (HIERARCHICAL_CATALOG_ASSETS, "source.hierarchy.catalog") => vec![
                coordinate_binding(
                    "media/hierarchy/item/1",
                    "segment:1",
                    subject_reference::Subject::ProviderItemKey(key("media.hierarchy.item.1")),
                    CoordinateBacking::Materialized,
                ),
                coordinate_binding(
                    "media/hierarchy/root",
                    "segment:1",
                    subject_reference::Subject::CatalogRelationKey(key(
                        "relation.media.hierarchy.group.1",
                    )),
                    CoordinateBacking::Virtual,
                ),
            ],
            (EMPTY_TARGETED_RECEIVER, "source.lookup.catalog") => {
                vec![coordinate_binding(
                    "media/linked/part1",
                    "segment:1..12",
                    subject_reference::Subject::ProviderItemKey(key("media.linked.part1")),
                    CoordinateBacking::Aggregate,
                )]
            }
            _ => Vec::new(),
        }
    }

    fn full_relations(&self, source: &str, items: &[ProviderItem]) -> Vec<CatalogRelation> {
        match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source == "source.hierarchy.catalog" => {
                hierarchical_relations("hierarchy", self.settings.revision == "v2")
            }
            EDITION_HIERARCHY => {
                let mut values = vec![
                    relation(
                        "relation.work.winter",
                        None,
                        Some("work.winter-archive"),
                        10,
                    ),
                    relation(
                        "relation.edition.winter.us",
                        Some("relation.work.winter"),
                        Some("edition.winter.us"),
                        10,
                    ),
                    relation(
                        "relation.edition.winter.uk",
                        Some("relation.work.winter"),
                        Some("edition.winter.uk"),
                        30,
                    ),
                    relation(
                        "relation.volume.winter.us.v1",
                        Some("relation.edition.winter.us"),
                        Some("volume.winter.us.v1"),
                        10,
                    ),
                    relation(
                        "relation.volume.winter.us.v2",
                        Some("relation.edition.winter.us"),
                        Some("volume.winter.us.v2"),
                        30,
                    ),
                    relation(
                        "relation.chapter.winter.us.v1.c1",
                        Some("relation.volume.winter.us.v1"),
                        Some("chapter.winter.us.v1.c1"),
                        10,
                    ),
                    relation(
                        "relation.chapter.winter.us.v1.c2",
                        Some("relation.volume.winter.us.v1"),
                        Some("chapter.winter.us.v1.c2"),
                        30,
                    ),
                ];
                if self.settings.revision == "v2" {
                    values.push(relation(
                        "relation.edition.winter.alt",
                        Some("relation.work.winter"),
                        Some("edition.winter.alt"),
                        50,
                    ));
                }
                values
            }
            ADVERSARIAL => adversarial_relations(self.settings.revision == "v2"),
            DUAL_CONNECTION_ISOLATION => isolated_connection_relations(
                &self.settings.instance,
                self.settings.revision == "v2",
            ),
            _ => relations_for(items),
        }
    }

    fn incremental_catalog(&self, source: &str) -> CatalogBatch {
        match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source == "source.hierarchy.catalog" => CatalogBatch {
                sequence: 0,
                item_upserts: vec![
                    item(
                        "media.hierarchy.item.1",
                        "media",
                        "Arrival Revised",
                        &["media/hierarchy/item/1"],
                    ),
                    item(
                        "media.hierarchy.item.3",
                        "media",
                        "Signal",
                        &["media/hierarchy/item/3"],
                    ),
                ],
                item_deletes: vec![key("media.hierarchy.item.2")],
                relation_upserts: vec![
                    relation(
                        "relation.media.hierarchy.special.moved",
                        Some("relation.media.hierarchy.group.1"),
                        Some("media.hierarchy.special"),
                        30,
                    ),
                    relation(
                        "relation.media.hierarchy.item.3",
                        Some("relation.media.hierarchy.group.1"),
                        Some("media.hierarchy.item.3"),
                        40,
                    ),
                ],
                relation_deletes: vec![
                    key("relation.media.hierarchy.special.root"),
                    key("relation.media.hierarchy.item.2"),
                ],
                ..CatalogBatch::default()
            },
            FLAT_CATALOG_REVISIONS => CatalogBatch {
                sequence: 0,
                item_upserts: vec![
                    item_with_attribute(
                        "media.flat.original",
                        "media",
                        "Shared Title",
                        &["media/flat/original"],
                        "release_date",
                        "2029-01-16",
                    ),
                    item_with_attribute(
                        "media.flat.revision",
                        "media",
                        "Shared Title",
                        &["media/flat/revision"],
                        "release_date",
                        "2030-02-02",
                    ),
                ],
                relation_upserts: vec![relation(
                    "relation.media.flat.revision",
                    None,
                    Some("media.flat.revision"),
                    30,
                )],
                ..CatalogBatch::default()
            },
            ADVERSARIAL => CatalogBatch {
                sequence: 0,
                relation_upserts: adversarial_relations(true),
                relation_deletes: adversarial_relations(false)
                    .into_iter()
                    .filter_map(|relation| relation.key)
                    .collect(),
                ..CatalogBatch::default()
            },
            DUAL_CONNECTION_ISOLATION => CatalogBatch {
                sequence: 0,
                item_upserts: vec![item(
                    &format!("media.{}.item.3", self.settings.instance),
                    "media",
                    "Item 3",
                    &[&format!("media/{}/item/3", self.settings.instance)],
                )],
                item_deletes: vec![key(&format!("media.{}.item.2", self.settings.instance))],
                relation_upserts: vec![relation(
                    &format!("relation.media.{}.item.3", self.settings.instance),
                    Some(&format!("relation.media.{}.group", self.settings.instance)),
                    Some(&format!("media.{}.item.3", self.settings.instance)),
                    30,
                )],
                relation_deletes: vec![key(&format!(
                    "relation.media.{}.item.2",
                    self.settings.instance
                ))],
                ..CatalogBatch::default()
            },
            _ => CatalogBatch::default(),
        }
    }

    fn full_state(&self, source: &str) -> StateBatch {
        let mut observations = match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source == "source.hierarchy.catalog" => vec![
                state_integer("media.hierarchy.item.1", "progress", "time", 1),
                state_boolean("media.hierarchy.item.1", "completed", true),
                if self.settings.revision == "v2" {
                    state_integer("media.hierarchy.item.3", "progress", "time", 120)
                } else {
                    state_integer("media.hierarchy.item.2", "progress", "time", 780)
                },
            ],
            HIERARCHICAL_CATALOG_ASSETS => vec![
                state_boolean("media.asset.primary", "watched", true),
                state_boolean("media.asset.primary", "completed", true),
            ],
            EMPTY_TARGETED_RECEIVER if source == "source.lookup.catalog" => {
                vec![state_integer(
                    "media.linked.part1",
                    "progress",
                    "time",
                    if self.settings.revision == "v2" { 8 } else { 7 },
                )]
            }
            EMPTY_TARGETED_RECEIVER if source == "source.reference.catalog" => vec![
                state_integer("media.reference.item", "progress", "time", 25),
                state_integer("media.reference.item", "rating", "points", 3),
            ],
            EDITION_HIERARCHY => vec![
                state_integer("edition.winter.us", "progress", "pages", 240),
                state_integer("edition.winter.us", "progress", "percent", 50),
                state_integer("volume.winter.us.v1", "progress", "chapters", 9),
                state_integer(
                    "volume.winter.us.v2",
                    "progress",
                    "chapters",
                    if self.settings.revision == "v2" { 2 } else { 0 },
                ),
            ],
            FLAT_CATALOG_REVISIONS => vec![
                state_boolean("media.flat.original", "watched", true),
                state_integer(
                    "media.flat.original",
                    "rating",
                    "points",
                    if self.settings.revision == "v2" { 9 } else { 8 },
                ),
                state_integer(
                    "media.flat.original",
                    "rewatch_count",
                    "count",
                    if self.settings.revision == "v2" { 3 } else { 2 },
                ),
            ],
            DUAL_CONNECTION_ISOLATION => vec![state_boolean(
                &format!("media.{}.item.1", self.settings.instance),
                "completed",
                self.settings.revision == "v2",
            )],
            _ => Vec::new(),
        };
        self.stamp_state(source, &mut observations);
        StateBatch {
            sequence: 0,
            observations,
        }
    }

    fn incremental_state(&self, source: &str) -> StateBatch {
        let mut observations = match self.settings.scenario_id.as_str() {
            HIERARCHICAL_CATALOG_ASSETS if source == "source.hierarchy.catalog" => vec![
                state_integer("media.hierarchy.item.3", "progress", "time", 120),
                state_deletion("media.hierarchy.item.2", "progress", "time"),
            ],
            EMPTY_TARGETED_RECEIVER if source == "source.lookup.catalog" => {
                vec![state_integer("media.linked.part1", "progress", "time", 8)]
            }
            FLAT_CATALOG_REVISIONS => vec![
                state_integer("media.flat.original", "rating", "points", 9),
                state_integer("media.flat.original", "revisit_count", "count", 3),
            ],
            DUAL_CONNECTION_ISOLATION => vec![state_boolean(
                &format!("media.{}.item.1", self.settings.instance),
                "completed",
                true,
            )],
            _ => Vec::new(),
        };
        self.stamp_state(source, &mut observations);
        StateBatch {
            sequence: 0,
            observations,
        }
    }

    fn stamp_state(&self, source: &str, observations: &mut [StateObservation]) {
        let revision = self.effective_revision(source, "state");
        for observation in observations {
            observation.provider_revision =
                format!("{}/{}/state/{revision}", self.settings.scenario_id, source).into_bytes();
            observation.observed_time_milliseconds =
                LOGICAL_TIME_MILLISECONDS + if revision == "v2" { 1_000 } else { 0 };
        }
    }

    fn lookup_outcome(
        &self,
        requested: &PortableReference,
    ) -> portable_reference_lookup_result::Outcome {
        let value = String::from_utf8_lossy(&requested.value);
        if self.settings.scenario_id == EMPTY_TARGETED_RECEIVER && value == "media/quiet-signal" {
            return portable_reference_lookup_result::Outcome::Matched(LookupMatched {
                candidate: Some(candidate(
                    "media.quiet-signal",
                    "Quiet Signal",
                    requested,
                    "lookup.receiver.v1",
                )),
            });
        }
        if self.settings.scenario_id == EMPTY_TARGETED_RECEIVER && value == "media/linked" {
            return portable_reference_lookup_result::Outcome::Ambiguous(LookupAmbiguous {
                candidates: vec![
                    candidate(
                        "media.linked.part1",
                        "Linked Part 1",
                        requested,
                        "lookup.receiver.v1",
                    ),
                    candidate(
                        "media.linked.part2",
                        "Linked Part 2",
                        requested,
                        "lookup.receiver.v1",
                    ),
                ],
            });
        }
        if self.settings.scenario_id == ADVERSARIAL {
            return match value.as_ref() {
                "duplicate" => {
                    portable_reference_lookup_result::Outcome::Ambiguous(LookupAmbiguous {
                        candidates: vec![
                            candidate(
                                "adversarial.item.left",
                                "Duplicate Left",
                                requested,
                                "lookup.adversarial.v1",
                            ),
                            candidate(
                                "adversarial.item.right",
                                "Duplicate Right",
                                requested,
                                "lookup.adversarial.v1",
                            ),
                        ],
                    })
                }
                "missing" => portable_reference_lookup_result::Outcome::NotFound(LookupNotFound {}),
                _ => portable_reference_lookup_result::Outcome::Unsupported(LookupUnsupported {}),
            };
        }
        portable_reference_lookup_result::Outcome::Unsupported(LookupUnsupported {})
    }

    fn endpoint_lookup_outcome(
        &self,
        requested: &PortableEndpoint,
    ) -> portable_endpoint_resolution::Outcome {
        let Some(reference) = requested.reference.as_ref() else {
            return portable_endpoint_resolution::Outcome::Unsupported(LookupUnsupported {});
        };
        let value = String::from_utf8_lossy(&reference.value);
        if self.settings.scenario_id == EMPTY_TARGETED_RECEIVER
            && matches!(value.as_ref(), "quiet-signal" | "media/quiet-signal")
            && requested.selector == "segment:1..12"
        {
            return portable_endpoint_resolution::Outcome::Matched(EndpointLookupMatched {
                candidate: Some(endpoint_candidate(
                    "media.quiet-signal",
                    "Quiet Signal",
                    requested,
                    CoordinateBacking::Aggregate,
                    "lookup.receiver.endpoint.v1",
                )),
            });
        }
        if self.settings.scenario_id == EMPTY_TARGETED_RECEIVER
            && value == "media/linked"
            && requested.selector == "segment:1..24"
        {
            return portable_endpoint_resolution::Outcome::Ambiguous(EndpointLookupAmbiguous {
                candidates: vec![
                    endpoint_candidate(
                        "media.linked.part1",
                        "Linked Part 1",
                        requested,
                        CoordinateBacking::Aggregate,
                        "lookup.receiver.endpoint.v1",
                    ),
                    endpoint_candidate(
                        "media.linked.part2",
                        "Linked Part 2",
                        requested,
                        CoordinateBacking::Aggregate,
                        "lookup.receiver.endpoint.v1",
                    ),
                ],
            });
        }
        if self.settings.scenario_id == ADVERSARIAL && value == "missing" {
            return portable_endpoint_resolution::Outcome::NotFound(LookupNotFound {});
        }
        portable_endpoint_resolution::Outcome::Unsupported(LookupUnsupported {})
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ScenarioError {
    #[error("unknown fixture scenario {0}")]
    UnknownScenario(String),
    #[error("unknown fixture revision {0}")]
    UnknownRevision(String),
    #[error("unknown fixture fault {0}")]
    UnknownFault(String),
    #[error("unknown fixture instance {0}")]
    UnknownInstance(String),
    #[error("fixture source is unknown")]
    UnknownSource,
    #[error("fixture read mode is unsupported")]
    UnsupportedRead,
    #[error("fixture cursor is stale")]
    StaleCursor,
    #[error("fixture key is not valid UTF-8")]
    InvalidKey,
}

fn source(
    source_key: &str,
    display_name: &str,
    kind: &str,
    account_key: Option<Key>,
    incremental: bool,
    state: bool,
    assets: bool,
) -> SourceSnapshot {
    let targeted_state_read = source_key == "source.receiver.empty";
    let targeted_state_write = targeted_state_read || source_key == "source.adversarial.lookup";
    SourceSnapshot {
        key: Some(key(source_key)),
        account_key,
        display_name: display_name.to_owned(),
        kind: Some(term(kind)),
        availability: SourceAvailability::Available as i32,
        capabilities: Some(SourceCapabilities {
            catalog: Some(ReadCapability {
                full: true,
                incremental,
            }),
            state: state.then_some(ReadCapability {
                full: true,
                incremental,
            }),
            assets: assets.then_some(AssetCapability {
                maximum_bytes: 1024,
                content_types: vec!["image/jpeg".to_owned()],
            }),
            coordinates: Some(CoordinateCapability {
                coordinate_ids: vec![
                    "segment".to_owned(),
                    "volume".to_owned(),
                    "chapter".to_owned(),
                    "page".to_owned(),
                ],
            }),
            state_fields: if targeted_state_read || targeted_state_write {
                let mut fields = vec![StateFieldDescriptor {
                    field: Some(Term {
                        namespace: "dev.trakkin.state".to_owned(),
                        name: "watched".to_owned(),
                    }),
                    unit: None,
                    value_kind: ConfigurationValueKind::Boolean as i32,
                    rating_scale: None,
                    numeric_range: None,
                    quantizer: StateFieldQuantizer::Unspecified as i32,
                }];
                if targeted_state_read {
                    fields.push(StateFieldDescriptor {
                        field: Some(Term {
                            namespace: "dev.trakkin.state".to_owned(),
                            name: "progress".to_owned(),
                        }),
                        unit: Some(Term {
                            namespace: "dev.trakkin.unit".to_owned(),
                            name: "time".to_owned(),
                        }),
                        value_kind: ConfigurationValueKind::Integer as i32,
                        rating_scale: None,
                        numeric_range: Some(StateFieldNumericRange {
                            minimum: "0".to_owned(),
                            maximum: "12".to_owned(),
                            step: "1".to_owned(),
                        }),
                        quantizer: StateFieldQuantizer::Exact as i32,
                    });
                }
                fields
            } else {
                Vec::new()
            },
            targeted_state_read: targeted_state_read.then_some(TargetedStateReadCapability {
                maximum_fields: 8,
                maximum_response_bytes: 65_536,
            }),
            targeted_state_write: targeted_state_write.then_some(TargetedStateWriteCapability {
                fields: {
                    let mut fields = vec![TargetedStateFieldWriteCapability {
                        field: Some(StateField {
                            field: Some(Term {
                                namespace: "dev.trakkin.state".to_owned(),
                                name: "watched".to_owned(),
                            }),
                            unit: None,
                        }),
                        set_supported: true,
                        clear_supported: targeted_state_read,
                    }];
                    if targeted_state_read {
                        fields.push(TargetedStateFieldWriteCapability {
                            field: Some(StateField {
                                field: Some(Term {
                                    namespace: "dev.trakkin.state".to_owned(),
                                    name: "progress".to_owned(),
                                }),
                                unit: Some(Term {
                                    namespace: "dev.trakkin.unit".to_owned(),
                                    name: "time".to_owned(),
                                }),
                            }),
                            set_supported: true,
                            clear_supported: true,
                        });
                    }
                    fields
                },
                may_create_source_membership: targeted_state_read,
                precondition_mode: if targeted_state_read {
                    TargetedStateWritePreconditionMode::ProviderToken
                } else {
                    TargetedStateWritePreconditionMode::HostRecheckOnly
                } as i32,
                idempotency_mode: TargetedStateWriteIdempotencyMode::StableKey as i32,
                maximum_fields: 8,
                maximum_request_bytes: 65_536,
                maximum_response_bytes: 65_536,
                maximum_receipt_bytes: 1_024,
            }),
        }),
    }
}

fn rejected_targeted_write(code: &str) -> WriteTargetedStateResponse {
    WriteTargetedStateResponse {
        status: TargetedStateWriteStatus::Rejected as i32,
        certainty: TargetedStateWriteCertainty::ConfirmedNotApplied as i32,
        retry_disposition: TargetedStateWriteRetryDisposition::NotRetryable as i32,
        membership_effect: TargetedStateMembershipEffect::Unchanged as i32,
        error: Some(operation_failure(
            code,
            "targeted state write precondition was not satisfied",
            false,
        )),
        ..WriteTargetedStateResponse::default()
    }
}

fn provider_item_subject(value: &str) -> SubjectReference {
    SubjectReference {
        subject: Some(subject_reference::Subject::ProviderItemKey(key(value))),
    }
}

fn item(id: &str, kind: &str, display_name: &str, references: &[&str]) -> ProviderItem {
    ProviderItem {
        key: Some(key(id)),
        kind: Some(term(kind)),
        display_name: display_name.to_owned(),
        portable_references: references.iter().map(|value| reference(value)).collect(),
        ..ProviderItem::default()
    }
}

fn item_with_attribute(
    id: &str,
    kind: &str,
    display_name: &str,
    references: &[&str],
    attribute: &str,
    text: &str,
) -> ProviderItem {
    let mut value = item(id, kind, display_name, references);
    value.attributes.push(Attribute {
        term: Some(term(attribute)),
        value: Some(Value {
            value: Some(value::Value::Text(text.to_owned())),
        }),
    });
    value
}

fn item_with_asset(
    id: &str,
    kind: &str,
    display_name: &str,
    references: &[&str],
    asset_key: &str,
) -> ProviderItem {
    let mut value = item(id, kind, display_name, references);
    value.assets.push(BinaryAssetReference {
        key: Some(key(asset_key)),
        kind: Some(term("poster")),
    });
    value
}

fn asset_item() -> ProviderItem {
    item_with_asset(
        "media.asset.primary",
        "media",
        "Asset Item",
        &["media/asset/primary"],
        "asset.poster.valid",
    )
}

fn isolated_connection_items(instance: &str, revision_two: bool) -> Vec<ProviderItem> {
    let mut values = vec![
        item_with_asset(
            &format!("media.{instance}.root"),
            "media",
            &format!("Root {instance}"),
            &[&format!("media/{instance}/root")],
            &format!("asset.{instance}.poster"),
        ),
        item(
            &format!("media.{instance}.group"),
            "group",
            "Group",
            &[&format!("media/{instance}/group")],
        ),
        item(
            &format!("media.{instance}.item.1"),
            "media",
            "Item 1",
            &[&format!("media/{instance}/item/1")],
        ),
    ];
    values.push(if revision_two {
        item(
            &format!("media.{instance}.item.3"),
            "media",
            "Item 3",
            &[&format!("media/{instance}/item/3")],
        )
    } else {
        item(
            &format!("media.{instance}.item.2"),
            "media",
            "Item 2",
            &[&format!("media/{instance}/item/2")],
        )
    });
    values
}

fn hierarchical_relations(name: &str, revision_two: bool) -> Vec<CatalogRelation> {
    let mut values = vec![
        relation(
            &format!("relation.media.{name}.root"),
            None,
            Some(&format!("media.{name}.root")),
            10,
        ),
        relation(
            &format!("relation.media.{name}.group.1"),
            Some(&format!("relation.media.{name}.root")),
            Some(&format!("media.{name}.group.1")),
            10,
        ),
        relation(
            &format!("relation.media.{name}.item.1"),
            Some(&format!("relation.media.{name}.group.1")),
            Some(&format!("media.{name}.item.1")),
            10,
        ),
    ];
    if revision_two {
        values.extend([
            relation(
                &format!("relation.media.{name}.item.3"),
                Some(&format!("relation.media.{name}.group.1")),
                Some(&format!("media.{name}.item.3")),
                40,
            ),
            relation(
                &format!("relation.media.{name}.special.moved"),
                Some(&format!("relation.media.{name}.group.1")),
                Some(&format!("media.{name}.special")),
                30,
            ),
        ]);
    } else {
        values.extend([
            relation(
                &format!("relation.media.{name}.item.2"),
                Some(&format!("relation.media.{name}.group.1")),
                Some(&format!("media.{name}.item.2")),
                20,
            ),
            relation(
                &format!("relation.media.{name}.special.root"),
                None,
                Some(&format!("media.{name}.special")),
                20,
            ),
        ]);
    }
    values
}

fn isolated_connection_relations(instance: &str, revision_two: bool) -> Vec<CatalogRelation> {
    let root_relation = format!("relation.media.{instance}.root");
    let group_relation = format!("relation.media.{instance}.group");
    let mut values = vec![
        relation(
            &root_relation,
            None,
            Some(&format!("media.{instance}.root")),
            10,
        ),
        relation(
            &group_relation,
            Some(&root_relation),
            Some(&format!("media.{instance}.group")),
            10,
        ),
        relation(
            &format!("relation.media.{instance}.item.1"),
            Some(&group_relation),
            Some(&format!("media.{instance}.item.1")),
            10,
        ),
    ];
    let item = if revision_two { 3 } else { 2 };
    values.push(relation(
        &format!("relation.media.{instance}.item.{item}"),
        Some(&group_relation),
        Some(&format!("media.{instance}.item.{item}")),
        30,
    ));
    values
}

fn adversarial_relations(revision_two: bool) -> Vec<CatalogRelation> {
    let revision = if revision_two { "v2" } else { "v1" };
    let group = format!("relation.adversarial.group.{revision}");
    let mut values = vec![relation(&group, None, None, 10)];
    let left_parent = if revision_two {
        let nested = format!("relation.adversarial.nested.{revision}");
        values.push(relation(&nested, Some(&group), None, 20));
        nested
    } else {
        group.clone()
    };
    values.extend([
        relation(
            &format!("relation.adversarial.left.{revision}"),
            Some(&left_parent),
            Some("adversarial.item.left"),
            if revision_two { 30 } else { 10 },
        ),
        relation(
            &format!("relation.adversarial.right.{revision}"),
            Some(&group),
            Some("adversarial.item.right"),
            if revision_two { 10 } else { 30 },
        ),
        relation(
            &format!("relation.adversarial.missing.{revision}"),
            None,
            Some("adversarial.item.missing"),
            50,
        ),
    ]);
    values
}

fn relations_for(items: &[ProviderItem]) -> Vec<CatalogRelation> {
    items
        .iter()
        .enumerate()
        .map(|(position, item)| {
            let id = text_key(item.key.as_ref().expect("fixture item key"))
                .expect("fixture item keys are UTF-8");
            relation(
                &format!("relation.{id}"),
                None,
                Some(id),
                (position as i64 + 1) * 10,
            )
        })
        .collect()
}

fn relation(
    id: &str,
    parent: Option<&str>,
    provider_item: Option<&str>,
    order: i64,
) -> CatalogRelation {
    CatalogRelation {
        key: Some(key(id)),
        parent_key: parent.map(key),
        provider_item_key: provider_item.map(key),
        kind: Some(term("contains")),
        order: vec![order],
        attributes: Vec::new(),
    }
}

fn state_integer(item: &str, field: &str, unit: &str, integer: i64) -> StateObservation {
    state(
        item,
        field,
        unit,
        state_observation::Observation::Value(Value {
            value: Some(value::Value::Integer(integer)),
        }),
    )
}

fn state_boolean(item: &str, field: &str, boolean: bool) -> StateObservation {
    state(
        item,
        field,
        "flag",
        state_observation::Observation::Value(Value {
            value: Some(value::Value::Boolean(boolean)),
        }),
    )
}

fn state_deletion(item: &str, field: &str, unit: &str) -> StateObservation {
    state(
        item,
        field,
        unit,
        state_observation::Observation::Deletion(Default::default()),
    )
}

fn state(
    item: &str,
    field: &str,
    unit: &str,
    observation: state_observation::Observation,
) -> StateObservation {
    StateObservation {
        subject: Some(SubjectReference {
            subject: Some(subject_reference::Subject::ProviderItemKey(key(item))),
        }),
        field: Some(StateField {
            field: Some(term(field)),
            unit: Some(term(unit)),
        }),
        observation: Some(observation),
        provider_revision: b"fixture-state-v1".to_vec(),
        observed_time_milliseconds: LOGICAL_TIME_MILLISECONDS,
    }
}

fn state_field_identity(field: &StateField) -> String {
    let semantic = field.field.as_ref().expect("validated state field term");
    let unit = field.unit.as_ref().map_or_else(String::new, |unit| {
        format!("{}:{}", unit.namespace, unit.name)
    });
    format!("{}:{}|{unit}", semantic.namespace, semantic.name)
}

fn candidate(
    id: &str,
    display_name: &str,
    requested: &PortableReference,
    revision: &str,
) -> LookupCandidate {
    LookupCandidate {
        provider_item: Some(item(id, "media", display_name, &[])),
        evidence: Some(LookupEvidence {
            adapter_revision: revision.as_bytes().to_vec(),
            observed_time_milliseconds: LOGICAL_TIME_MILLISECONDS,
            expires_time_milliseconds: None,
            matched_references: vec![requested.clone()],
        }),
    }
}

fn endpoint_candidate(
    id: &str,
    display_name: &str,
    requested: &PortableEndpoint,
    backing: CoordinateBacking,
    revision: &str,
) -> EndpointLookupCandidate {
    let reference = requested
        .reference
        .clone()
        .expect("fixture endpoint reference is present");
    EndpointLookupCandidate {
        provider_item: Some(item(id, "media", display_name, &[])),
        binding: Some(CoordinateBinding {
            endpoint: Some(requested.clone()),
            subject: Some(SubjectReference {
                subject: Some(subject_reference::Subject::ProviderItemKey(key(id))),
            }),
            backing: backing as i32,
            evidence_revision: revision.as_bytes().to_vec(),
        }),
        evidence: Some(LookupEvidence {
            adapter_revision: revision.as_bytes().to_vec(),
            observed_time_milliseconds: LOGICAL_TIME_MILLISECONDS,
            expires_time_milliseconds: Some(LOGICAL_TIME_MILLISECONDS + 60_000),
            matched_references: vec![reference],
        }),
    }
}

fn coordinate_binding(
    reference_value: &str,
    selector: &str,
    subject: subject_reference::Subject,
    backing: CoordinateBacking,
) -> CoordinateBinding {
    CoordinateBinding {
        endpoint: Some(PortableEndpoint {
            reference: Some(reference(reference_value)),
            selector: selector.to_owned(),
        }),
        subject: Some(SubjectReference {
            subject: Some(subject),
        }),
        backing: backing as i32,
        evidence_revision: b"fixtures.coordinates/v1".to_vec(),
    }
}

fn failed_catalog(code: &str, retryable: bool) -> Vec<ReadCatalogResponse> {
    vec![ReadCatalogResponse {
        event: Some(read_catalog_response::Event::Failed(ReadFailed {
            error: Some(error(code, retryable)),
        })),
    }]
}

fn failed_state(code: &str, retryable: bool) -> Vec<ReadStateResponse> {
    vec![ReadStateResponse {
        event: Some(read_state_response::Event::Failed(ReadFailed {
            error: Some(error(code, retryable)),
        })),
    }]
}

fn error_asset(code: &str, retryable: bool) -> ReadAssetResponse {
    ReadAssetResponse {
        outcome: Some(read_asset_response::Outcome::Error(error(code, retryable))),
    }
}

fn error(code: &str, retryable: bool) -> OperationFailure {
    operation_failure(code, &code.replace('_', " "), retryable)
}

fn key(value: &str) -> Key {
    Key {
        namespace: "fixture".to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

fn term(name: &str) -> Term {
    Term {
        namespace: "trakkin".to_owned(),
        name: name.to_owned(),
    }
}

fn reference(value: &str) -> PortableReference {
    PortableReference {
        namespace: "example.media".to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

fn cursor(scenario: &str, source: &str, stream: &str, revision: &str) -> Vec<u8> {
    format!("{scenario}/{source}/{stream}/{revision}").into_bytes()
}

fn text_key(key: &Key) -> Result<&str, ScenarioError> {
    std::str::from_utf8(&key.value).map_err(|_| ScenarioError::InvalidKey)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use trakkin_provider_sdk::{
        v1::{
            CoordinateBacking, PortableEndpoint, ReadMode, ReadTargetedStateRequest,
            SourceMembership, StateField, StatePresence, TargetedStateWriteCertainty,
            TargetedStateWriteIdempotencyMode, TargetedStateWriteIntent,
            TargetedStateWritePreconditionMode, TargetedStateWriteStatus, Term, Value,
            WriteTargetedStateRequest, lookup_portable_references_response,
            portable_endpoint_resolution, portable_reference_lookup_result, read_catalog_response,
            read_state_response, read_targeted_state_response, resolve_portable_endpoints_response,
            state_observation, targeted_state_write_intent, value,
        },
        validation::{
            CatalogStreamValidator, asset_response, lookup_response, read_targeted_state_response,
            resolve_endpoints_response, targeted_state_read_request, targeted_state_write_request,
            targeted_state_write_response,
        },
    };

    use super::{
        ADVERSARIAL, EDITION_HIERARCHY, EMPTY_TARGETED_RECEIVER, FLAT_CATALOG_REVISIONS,
        FixtureSettings, HIERARCHICAL_CATALOG_ASSETS, Scenario, ScenarioError, key, reference,
    };

    #[test]
    fn empty_receiver_lookup_resolves_without_membership() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: EMPTY_TARGETED_RECEIVER.to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let receiver = scenario
            .sources()
            .into_iter()
            .find(|source| source.display_name == "Empty Receiver")
            .unwrap();
        let events = scenario
            .catalog_events(receiver.key.as_ref().unwrap(), ReadMode::Full, &[])
            .unwrap();
        let batch = match events[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => batch,
            _ => panic!("expected catalog batch"),
        };
        assert!(batch.item_upserts.is_empty());

        let requested = reference("media/quiet-signal");
        let response = scenario.lookup(std::slice::from_ref(&requested));
        lookup_response(&[requested], &response).unwrap();
        let Some(lookup_portable_references_response::Outcome::Result(result)) =
            response.outcome.as_ref()
        else {
            panic!("fixture lookup failed");
        };
        assert!(matches!(
            result.results[0].outcome,
            Some(portable_reference_lookup_result::Outcome::Matched(_))
        ));

        let requested_endpoint = PortableEndpoint {
            reference: Some(reference("media/quiet-signal")),
            selector: "segment:1..12".to_owned(),
        };
        let response = scenario.resolve_endpoints(std::slice::from_ref(&requested_endpoint));
        resolve_endpoints_response(std::slice::from_ref(&requested_endpoint), &response, 4096)
            .unwrap();
        let Some(resolve_portable_endpoints_response::Outcome::Result(result)) =
            response.outcome.as_ref()
        else {
            panic!("fixture endpoint resolution failed");
        };
        let candidate = match result.results[0].outcome.as_ref().unwrap() {
            portable_endpoint_resolution::Outcome::Matched(matched) => {
                matched.candidate.as_ref().unwrap()
            }
            _ => panic!("expected matched endpoint"),
        };
        assert_eq!(
            candidate.binding.as_ref().unwrap().backing,
            CoordinateBacking::Aggregate as i32
        );
    }

    #[test]
    fn empty_receiver_targeted_read_returns_absent_preflight() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: EMPTY_TARGETED_RECEIVER.to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let receiver = scenario
            .sources()
            .into_iter()
            .find(|source| source.display_name == "Empty Receiver")
            .unwrap();
        let capability = receiver
            .capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.targeted_state_read.as_ref())
            .unwrap();
        let field = StateField {
            field: Some(Term {
                namespace: "dev.trakkin.state".to_owned(),
                name: "watched".to_owned(),
            }),
            unit: None,
        };
        let request = ReadTargetedStateRequest {
            operation_id: b"fixture-targeted-read".to_vec(),
            source_key: receiver.key,
            subject: Some(super::provider_item_subject("media.quiet-signal")),
            fields: vec![field],
            maximum_response_bytes: 4096,
            reconciliation_idempotency_key: Vec::new(),
        };
        targeted_state_read_request(&request, capability).unwrap();
        let response = scenario.targeted_state(&request);
        read_targeted_state_response(&request, &response).unwrap();
        let read_targeted_state_response::Outcome::Matched(matched) = response.outcome.unwrap()
        else {
            panic!("targeted state should match outside source membership")
        };
        assert_eq!(matched.membership, SourceMembership::Absent as i32);
        assert_eq!(matched.fields[0].presence, StatePresence::Absent as i32);
        assert!(matched.fields[0].value.is_none());
        assert_eq!(matched.precondition, b"expected-absent-r1");
        assert!(matched.expires_time_milliseconds.unwrap() > matched.observed_time_milliseconds);
    }

    #[test]
    fn empty_receiver_write_creates_membership_idempotently() {
        let mut scenario = Scenario::load(FixtureSettings {
            scenario_id: EMPTY_TARGETED_RECEIVER.to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let receiver = scenario
            .sources()
            .into_iter()
            .find(|source| source.display_name == "Empty Receiver")
            .unwrap();
        let capability = receiver
            .capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.targeted_state_write.clone())
            .unwrap();
        assert_eq!(
            capability.precondition_mode,
            TargetedStateWritePreconditionMode::ProviderToken as i32
        );
        assert_eq!(
            capability.idempotency_mode,
            TargetedStateWriteIdempotencyMode::StableKey as i32
        );
        let field = StateField {
            field: Some(Term {
                namespace: "dev.trakkin.state".to_owned(),
                name: "watched".to_owned(),
            }),
            unit: None,
        };
        let request = WriteTargetedStateRequest {
            operation_id: b"fixture-targeted-write".to_vec(),
            source_key: receiver.key,
            subject: Some(super::provider_item_subject("media.quiet-signal")),
            idempotency_key: b"sync-action-empty".to_vec(),
            expected_membership: SourceMembership::Absent as i32,
            precondition: b"expected-absent-r1".to_vec(),
            allow_create_membership: true,
            intents: vec![TargetedStateWriteIntent {
                field: Some(field.clone()),
                operation: Some(targeted_state_write_intent::Operation::Set(Value {
                    value: Some(value::Value::Boolean(true)),
                })),
            }],
            maximum_receipt_bytes: 64,
            maximum_response_bytes: 4096,
        };
        targeted_state_write_request(&request, &capability).unwrap();
        let response = scenario.write_targeted_state(&request);
        targeted_state_write_response(&request, &capability, &response).unwrap();
        assert_eq!(response.status, TargetedStateWriteStatus::Applied as i32);
        assert_eq!(
            response.certainty,
            TargetedStateWriteCertainty::ConfirmedApplied as i32
        );
        assert!(!response.receipt.is_empty());
        assert_eq!(scenario.write_targeted_state(&request), response);

        let read = ReadTargetedStateRequest {
            operation_id: b"fixture-targeted-read-after-write".to_vec(),
            source_key: request.source_key,
            subject: request.subject,
            fields: vec![field],
            maximum_response_bytes: 4096,
            reconciliation_idempotency_key: Vec::new(),
        };
        let response = scenario.targeted_state(&read);
        let read_targeted_state_response::Outcome::Matched(matched) = response.outcome.unwrap()
        else {
            panic!("targeted state should remain addressable after membership creation")
        };
        assert_eq!(matched.membership, SourceMembership::Present as i32);
        assert_eq!(matched.fields[0].presence, StatePresence::Present as i32);
        assert_eq!(
            matched.fields[0].value.as_ref().unwrap().value,
            Some(value::Value::Boolean(true))
        );
        assert_eq!(matched.precondition, b"expected-present-r2");

        let adversarial = Scenario::load(FixtureSettings {
            scenario_id: ADVERSARIAL.to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let weak = adversarial
            .sources()
            .into_iter()
            .find(|source| source.key.as_ref() == Some(&key("source.adversarial.lookup")))
            .and_then(|source| source.capabilities)
            .and_then(|capabilities| capabilities.targeted_state_write)
            .unwrap();
        assert_eq!(
            weak.precondition_mode,
            TargetedStateWritePreconditionMode::HostRecheckOnly as i32
        );
    }

    #[test]
    fn hierarchical_incremental_delete_and_asset_are_deterministic() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: HIERARCHICAL_CATALOG_ASSETS.to_owned(),
            revision: "v2".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let source = key("source.hierarchy.catalog");
        let prior = format!("{HIERARCHICAL_CATALOG_ASSETS}/source.hierarchy.catalog/catalog/v1")
            .into_bytes();
        let events = scenario
            .catalog_events(&source, ReadMode::Incremental, &prior)
            .unwrap();
        let mut validator = CatalogStreamValidator::default();
        for event in &events {
            validator.accept(event).unwrap();
        }
        validator.finish().unwrap();
        let batch = match events[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => batch,
            _ => panic!("expected catalog batch"),
        };
        assert_eq!(batch.item_deletes, vec![key("media.hierarchy.item.2")]);

        let prior =
            format!("{HIERARCHICAL_CATALOG_ASSETS}/source.hierarchy.catalog/state/v1").into_bytes();
        let state_events = scenario
            .state_events(&source, ReadMode::Incremental, &prior)
            .unwrap();
        let state_batch = match state_events[0].event.as_ref().unwrap() {
            read_state_response::Event::Batch(batch) => batch,
            _ => panic!("expected state batch"),
        };
        assert!(state_batch.observations.iter().any(|observation| matches!(
            observation.observation,
            Some(state_observation::Observation::Deletion(_))
        )));

        let asset = scenario.asset(
            &key("source.assets.catalog"),
            &key("media.asset.primary"),
            &key("asset.poster.valid"),
            1024,
        );
        asset_response(&asset, 1024, &["image/jpeg".to_owned()]).unwrap();
    }

    #[test]
    fn adversarial_faults_and_lookup_outcomes_are_explicit() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: ADVERSARIAL.to_owned(),
            fault: "read.malformed-catalog".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let events = scenario
            .catalog_events(&key("source.adversarial.catalog"), ReadMode::Full, &[])
            .unwrap();
        assert!(
            CatalogStreamValidator::default()
                .accept(&events[0])
                .is_err()
        );

        let references = vec![
            reference("missing"),
            reference("duplicate"),
            reference("other"),
        ];
        let response = scenario.lookup(&references);
        lookup_response(&references, &response).unwrap();
        let Some(lookup_portable_references_response::Outcome::Result(result)) =
            response.outcome.as_ref()
        else {
            panic!("fixture lookup failed");
        };
        assert!(matches!(
            result.results[0].outcome,
            Some(portable_reference_lookup_result::Outcome::NotFound(_))
        ));
        assert!(matches!(
            result.results[1].outcome,
            Some(portable_reference_lookup_result::Outcome::Ambiguous(_))
        ));
        assert!(matches!(
            result.results[2].outcome,
            Some(portable_reference_lookup_result::Outcome::Unsupported(_))
        ));
    }

    #[test]
    fn lookup_catalog_revision_advances_only_its_state_stream() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: EMPTY_TARGETED_RECEIVER.to_owned(),
            revision: "v2".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let catalog = scenario
            .catalog_events(&key("source.lookup.catalog"), ReadMode::Full, &[])
            .unwrap();
        let catalog_cursor = match catalog.last().unwrap().event.as_ref().unwrap() {
            read_catalog_response::Event::Completed(completed) => &completed.next_cursor,
            _ => panic!("expected completed catalog read"),
        };
        assert!(catalog_cursor.ends_with(b"/v1"));

        let state = scenario
            .state_events(&key("source.lookup.catalog"), ReadMode::Full, &[])
            .unwrap();
        let batch = match state[0].event.as_ref().unwrap() {
            read_state_response::Event::Batch(batch) => batch,
            _ => panic!("expected state batch"),
        };
        assert!(batch.observations[0].provider_revision.ends_with(b"/v2"));
        let state_cursor = match state.last().unwrap().event.as_ref().unwrap() {
            read_state_response::Event::Completed(completed) => &completed.next_cursor,
            _ => panic!("expected completed state read"),
        };
        assert!(state_cursor.ends_with(b"/v2"));
    }

    #[test]
    fn edition_hierarchy_is_full_only_and_revision_adds_items() {
        let scenario = Scenario::load(FixtureSettings {
            scenario_id: EDITION_HIERARCHY.to_owned(),
            revision: "v2".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let source = scenario.sources().remove(0);
        let capabilities = source.capabilities.unwrap();
        assert!(!capabilities.catalog.unwrap().incremental);
        assert!(!capabilities.state.unwrap().incremental);
        assert_eq!(
            scenario.catalog_events(
                source.key.as_ref().unwrap(),
                ReadMode::Incremental,
                b"cursor",
            ),
            Err(ScenarioError::UnsupportedRead)
        );
        let events = scenario
            .catalog_events(source.key.as_ref().unwrap(), ReadMode::Full, &[])
            .unwrap();
        let batch = match events[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => batch,
            _ => panic!("expected catalog batch"),
        };
        let item_keys = batch
            .item_upserts
            .iter()
            .map(|item| item.key.as_ref().unwrap())
            .collect::<HashSet<_>>();
        assert!(item_keys.contains(&key("chapter.winter.us.v1.c2")));
        assert!(item_keys.contains(&key("edition.winter.alt")));
    }

    #[test]
    fn flat_and_adversarial_scenarios_publish_declared_revision_changes() {
        let flat = Scenario::load(FixtureSettings {
            scenario_id: FLAT_CATALOG_REVISIONS.to_owned(),
            revision: "v2".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let flat_events = flat
            .catalog_events(
                &key("source.flat.catalog"),
                ReadMode::Incremental,
                format!("{FLAT_CATALOG_REVISIONS}/source.flat.catalog/catalog/v1").as_bytes(),
            )
            .unwrap();
        let flat_batch = match flat_events[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => batch,
            _ => panic!("expected flat catalog batch"),
        };
        assert_eq!(flat_batch.item_upserts.len(), 2);
        assert!(
            flat_batch
                .item_upserts
                .iter()
                .any(|item| { item.key.as_ref() == Some(&key("media.flat.revision")) })
        );
        let flat_state = flat
            .state_events(
                &key("source.flat.catalog"),
                ReadMode::Incremental,
                format!("{FLAT_CATALOG_REVISIONS}/source.flat.catalog/state/v1").as_bytes(),
            )
            .unwrap();
        let flat_state_batch = match flat_state[0].event.as_ref().unwrap() {
            read_state_response::Event::Batch(batch) => batch,
            _ => panic!("expected flat state batch"),
        };
        assert_eq!(flat_state_batch.observations.len(), 2);

        let adversarial_v1 = Scenario::load(FixtureSettings {
            scenario_id: ADVERSARIAL.to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let adversarial_v2 = Scenario::load(FixtureSettings {
            scenario_id: ADVERSARIAL.to_owned(),
            revision: "v2".to_owned(),
            ..FixtureSettings::default()
        })
        .unwrap();
        let v1 = adversarial_v1
            .catalog_events(&key("source.adversarial.catalog"), ReadMode::Full, &[])
            .unwrap();
        let v2 = adversarial_v2
            .catalog_events(&key("source.adversarial.catalog"), ReadMode::Full, &[])
            .unwrap();
        let v1_relations = match v1[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => &batch.relation_upserts,
            _ => panic!("expected adversarial v1 batch"),
        };
        let v2_relations = match v2[0].event.as_ref().unwrap() {
            read_catalog_response::Event::Batch(batch) => &batch.relation_upserts,
            _ => panic!("expected adversarial v2 batch"),
        };
        assert_ne!(v1_relations, v2_relations);
        assert!(v2_relations.iter().any(|relation| {
            relation.provider_item_key.is_none() && relation.parent_key.is_some()
        }));
    }

    #[test]
    fn unknown_fault_is_rejected() {
        assert!(matches!(
            Scenario::load(FixtureSettings {
                fault: "unknown".to_owned(),
                ..FixtureSettings::default()
            }),
            Err(ScenarioError::UnknownFault(_))
        ));
    }
}

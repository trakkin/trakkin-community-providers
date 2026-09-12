use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use trakkin_provider_sdk::v1::{
    Attribute, BinaryAssetReference, CatalogRelation, ConfigurationValueKind, DecimalValue, Key,
    PortableReference, ProviderItem, RatingScale, StateDeletion, StateField, StateFieldDescriptor,
    StateFieldNumericRange, StateFieldQuantizer, StateObservation, SubjectReference, Term, Value,
    state_observation, subject_reference, value,
};

use crate::model::MediaItem;

pub const KEY_NAMESPACE: &str = "plex";
pub const MEDIA_NAMESPACE: &str = "dev.trakkin.media";
pub const STATE_NAMESPACE: &str = "dev.trakkin.state";
pub const UNIT_NAMESPACE: &str = "dev.trakkin.unit";

pub const WATCHED_FIELD: &str = "watched";
pub const PROGRESS_FIELD: &str = "progress";
pub const RATING_FIELD: &str = "rating";

pub fn key(value: impl AsRef<[u8]>) -> Key {
    Key {
        namespace: KEY_NAMESPACE.to_owned(),
        value: value.as_ref().to_vec(),
    }
}

pub fn account_key(machine_identifier: &str) -> Key {
    key(format!("account:{machine_identifier}"))
}

pub fn source_key(section_key: &str) -> Key {
    key(format!("library:{section_key}"))
}

pub fn item_key(rating_key: &str) -> Key {
    key(format!("item:{rating_key}"))
}

pub fn relation_key(rating_key: &str) -> Key {
    key(format!("relation:{rating_key}"))
}

pub fn parse_source_key(value: &Key) -> Option<&str> {
    parse_prefixed_key(value, "library:")
}

pub fn parse_item_key(value: &Key) -> Option<&str> {
    parse_prefixed_key(value, "item:")
}

fn parse_prefixed_key<'a>(value: &'a Key, prefix: &str) -> Option<&'a str> {
    if value.namespace != KEY_NAMESPACE {
        return None;
    }
    std::str::from_utf8(&value.value).ok()?.strip_prefix(prefix)
}

pub fn term(namespace: &str, name: impl Into<String>) -> Term {
    Term {
        namespace: namespace.to_owned(),
        name: name.into(),
    }
}

pub fn provider_item(item: &MediaItem) -> ProviderItem {
    let mut attributes = Vec::new();
    push_text_attribute(&mut attributes, "summary", item.summary.as_deref());
    push_integer_attribute(&mut attributes, "duration", item.duration);
    push_text_attribute(
        &mut attributes,
        "originally_available_at",
        item.originally_available_at.as_deref(),
    );
    push_integer_attribute(&mut attributes, "year", item.year);
    push_integer_attribute(&mut attributes, "index", item.index);
    push_integer_attribute(&mut attributes, "parent_index", item.parent_index);
    for genre in &item.genres {
        push_text_attribute(&mut attributes, "genre", Some(&genre.tag));
    }

    let mut assets = Vec::new();
    if non_empty(item.thumb.as_deref()).is_some() {
        assets.push(asset_reference("poster"));
    }
    if non_empty(item.art.as_deref()).is_some() {
        assets.push(asset_reference("backdrop"));
    }

    ProviderItem {
        key: Some(item_key(&item.rating_key)),
        kind: Some(term(MEDIA_NAMESPACE, item.media_type.clone())),
        display_name: display_name(item),
        attributes,
        portable_references: portable_references(item),
        assets,
    }
}

pub fn catalog_relation(item: &MediaItem, position: usize) -> CatalogRelation {
    CatalogRelation {
        key: Some(relation_key(&item.rating_key)),
        parent_key: item.parent_rating_key.as_deref().map(relation_key),
        provider_item_key: Some(item_key(&item.rating_key)),
        kind: Some(term("dev.trakkin.relation", "contains")),
        order: vec![item.index.unwrap_or(position as i64)],
        attributes: Vec::new(),
    }
}

pub fn portable_references(item: &MediaItem) -> Vec<PortableReference> {
    let mut seen = HashSet::new();
    item.guid
        .iter()
        .chain(item.guids.iter().map(|guid| &guid.id))
        .filter_map(|guid| portable_reference(guid))
        .filter(|reference| seen.insert((reference.namespace.clone(), reference.value.clone())))
        .collect()
}

pub fn portable_reference(guid: &str) -> Option<PortableReference> {
    let (namespace, value) = guid.split_once("://")?;
    if namespace.is_empty() || value.is_empty() {
        return None;
    }
    Some(PortableReference {
        namespace: namespace.to_ascii_lowercase(),
        value: value.as_bytes().to_vec(),
    })
}

pub fn state_fields() -> Vec<StateFieldDescriptor> {
    vec![
        StateFieldDescriptor {
            field: Some(term(STATE_NAMESPACE, WATCHED_FIELD)),
            value_kind: ConfigurationValueKind::Boolean as i32,
            ..StateFieldDescriptor::default()
        },
        StateFieldDescriptor {
            field: Some(term(STATE_NAMESPACE, PROGRESS_FIELD)),
            unit: Some(term(UNIT_NAMESPACE, "millisecond")),
            value_kind: ConfigurationValueKind::Integer as i32,
            numeric_range: Some(StateFieldNumericRange {
                minimum: "0".to_owned(),
                maximum: i64::MAX.to_string(),
                step: "1".to_owned(),
            }),
            quantizer: StateFieldQuantizer::Exact as i32,
            ..StateFieldDescriptor::default()
        },
        StateFieldDescriptor {
            field: Some(term(STATE_NAMESPACE, RATING_FIELD)),
            value_kind: ConfigurationValueKind::Decimal as i32,
            rating_scale: Some(RatingScale {
                minimum: "0".to_owned(),
                maximum: "10".to_owned(),
                step: "0.5".to_owned(),
            }),
            ..StateFieldDescriptor::default()
        },
    ]
}

pub fn watched_field() -> StateField {
    StateField {
        field: Some(term(STATE_NAMESPACE, WATCHED_FIELD)),
        unit: None,
    }
}

pub fn progress_field() -> StateField {
    StateField {
        field: Some(term(STATE_NAMESPACE, PROGRESS_FIELD)),
        unit: Some(term(UNIT_NAMESPACE, "millisecond")),
    }
}

pub fn rating_field() -> StateField {
    StateField {
        field: Some(term(STATE_NAMESPACE, RATING_FIELD)),
        unit: None,
    }
}

pub fn state_observations(item: &MediaItem, observed_at: i64) -> Vec<StateObservation> {
    let revision = provider_revision(item);
    vec![
        observation(
            item,
            watched_field(),
            state_observation::Observation::Value(boolean_value(
                item.view_count.unwrap_or_default() > 0,
            )),
            &revision,
            observed_at,
        ),
        observation(
            item,
            progress_field(),
            state_observation::Observation::Value(integer_value(
                item.view_offset.unwrap_or_default().max(0),
            )),
            &revision,
            observed_at,
        ),
        observation(
            item,
            rating_field(),
            item.user_rating.map_or_else(
                || state_observation::Observation::Deletion(StateDeletion {}),
                |rating| state_observation::Observation::Value(decimal_value(rating)),
            ),
            &revision,
            observed_at,
        ),
    ]
}

pub fn value_for_field(item: &MediaItem, field: &StateField) -> Option<Value> {
    if field == &watched_field() {
        Some(boolean_value(item.view_count.unwrap_or_default() > 0))
    } else if field == &progress_field() {
        Some(integer_value(item.view_offset.unwrap_or_default().max(0)))
    } else if field == &rating_field() {
        item.user_rating.map(decimal_value)
    } else {
        None
    }
}

pub fn provider_revision(item: &MediaItem) -> Vec<u8> {
    format!(
        "{}:{}:{}:{}:{}",
        item.updated_at.unwrap_or_default(),
        item.last_viewed_at.unwrap_or_default(),
        item.view_count.unwrap_or_default(),
        item.view_offset.unwrap_or_default(),
        item.user_rating
            .map(|value| value.to_string())
            .unwrap_or_default(),
    )
    .into_bytes()
}

pub fn now_milliseconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub fn asset_kind(asset_key: &Key) -> Option<&str> {
    let value = parse_prefixed_key(asset_key, "asset:")?;
    match value {
        "poster" | "backdrop" => Some(value),
        _ => None,
    }
}

fn display_name(item: &MediaItem) -> String {
    match (&item.grandparent_title, &item.parent_title) {
        (Some(grandparent), Some(parent)) if !grandparent.is_empty() && !parent.is_empty() => {
            format!("{grandparent} - {parent} - {}", item.title)
        }
        (_, Some(parent)) if !parent.is_empty() => format!("{parent} - {}", item.title),
        _ => item.title.clone(),
    }
}

fn asset_reference(kind: &str) -> BinaryAssetReference {
    BinaryAssetReference {
        key: Some(key(format!("asset:{kind}"))),
        kind: Some(term("dev.trakkin.asset", kind)),
    }
}

fn observation(
    item: &MediaItem,
    field: StateField,
    value: state_observation::Observation,
    revision: &[u8],
    observed_at: i64,
) -> StateObservation {
    StateObservation {
        subject: Some(SubjectReference {
            subject: Some(subject_reference::Subject::ProviderItemKey(item_key(
                &item.rating_key,
            ))),
        }),
        field: Some(field),
        observation: Some(value),
        provider_revision: revision.to_vec(),
        observed_time_milliseconds: observed_at,
    }
}

fn push_text_attribute(attributes: &mut Vec<Attribute>, name: &str, text: Option<&str>) {
    if let Some(text) = non_empty(text) {
        attributes.push(Attribute {
            term: Some(term(MEDIA_NAMESPACE, name)),
            value: Some(text_value(text)),
        });
    }
}

fn push_integer_attribute(attributes: &mut Vec<Attribute>, name: &str, number: Option<i64>) {
    if let Some(number) = number {
        attributes.push(Attribute {
            term: Some(term(MEDIA_NAMESPACE, name)),
            value: Some(integer_value(number)),
        });
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn text_value(text: &str) -> Value {
    Value {
        value: Some(value::Value::Text(text.to_owned())),
    }
}

pub fn boolean_value(boolean: bool) -> Value {
    Value {
        value: Some(value::Value::Boolean(boolean)),
    }
}

pub fn integer_value(integer: i64) -> Value {
    Value {
        value: Some(value::Value::Integer(integer)),
    }
}

pub fn decimal_value(decimal: f64) -> Value {
    Value {
        value: Some(value::Value::Decimal(DecimalValue {
            value: decimal.to_string(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PlexGuid, Tag};

    fn episode() -> MediaItem {
        MediaItem {
            rating_key: "42".to_owned(),
            key: "/library/metadata/42".to_owned(),
            parent_rating_key: Some("7".to_owned()),
            grandparent_rating_key: Some("3".to_owned()),
            guid: Some("plex://episode/abc".to_owned()),
            media_type: "episode".to_owned(),
            title: "Pilot".to_owned(),
            parent_title: Some("Season 1".to_owned()),
            grandparent_title: Some("A Show".to_owned()),
            summary: Some("Summary".to_owned()),
            thumb: Some("/library/metadata/42/thumb/1".to_owned()),
            art: None,
            duration: Some(1_800_000),
            added_at: Some(1),
            updated_at: Some(2),
            originally_available_at: Some("2024-01-01".to_owned()),
            year: Some(2024),
            index: Some(1),
            parent_index: Some(1),
            user_rating: Some(8.5),
            view_count: Some(1),
            view_offset: Some(900_000),
            last_viewed_at: Some(3),
            guids: vec![PlexGuid {
                id: "imdb://tt123".to_owned(),
            }],
            genres: vec![Tag {
                tag: "Drama".to_owned(),
            }],
        }
    }

    #[test]
    fn maps_hierarchy_references_assets_and_state() {
        let episode = episode();
        let item = provider_item(&episode);
        assert_eq!(item.display_name, "A Show - Season 1 - Pilot");
        assert_eq!(item.portable_references.len(), 2);
        assert_eq!(item.assets.len(), 1);
        assert_eq!(item.assets[0].key, Some(key("asset:poster")));

        let relation = catalog_relation(&episode, 9);
        assert_eq!(relation.parent_key, Some(relation_key("7")));
        assert_eq!(relation.order, vec![1]);

        let state = state_observations(&episode, 10);
        assert_eq!(state.len(), 3);
        assert!(matches!(
            state[0].observation,
            Some(state_observation::Observation::Value(Value {
                value: Some(value::Value::Boolean(true))
            }))
        ));
    }

    #[test]
    fn provider_revision_tracks_watched_state() {
        let mut episode = episode();
        let unwatched_revision = provider_revision(&episode);

        episode.view_count = Some(2);

        assert_ne!(provider_revision(&episode), unwatched_revision);
    }

    #[test]
    fn rejects_foreign_and_malformed_keys() {
        assert_eq!(parse_item_key(&item_key("42")), Some("42"));
        assert_eq!(parse_source_key(&source_key("1")), Some("1"));
        assert_eq!(parse_item_key(&key("library:1")), None);
        assert_eq!(
            parse_item_key(&Key {
                namespace: "other".to_owned(),
                value: b"item:42".to_vec(),
            }),
            None
        );
    }
}

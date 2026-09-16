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

pub const PLEX_REFERENCE_NAMESPACE: &str = "tv.plex";
pub const IMDB_REFERENCE_NAMESPACE: &str = "com.imdb";
pub const TMDB_REFERENCE_NAMESPACE: &str = "org.themoviedb";
pub const TVDB_REFERENCE_NAMESPACE: &str = "com.thetvdb";
pub const MUSICBRAINZ_REFERENCE_NAMESPACE: &str = "org.musicbrainz";
pub const REFERENCE_NAMESPACES: [&str; 5] = [
    PLEX_REFERENCE_NAMESPACE,
    IMDB_REFERENCE_NAMESPACE,
    TMDB_REFERENCE_NAMESPACE,
    TVDB_REFERENCE_NAMESPACE,
    MUSICBRAINZ_REFERENCE_NAMESPACE,
];

pub const WATCHED_FIELD: &str = "watched";
pub const PROGRESS_FIELD: &str = "progress";
pub const RATING_FIELD: &str = "rating";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecommendationPolicy {
    #[default]
    None,
    Movie,
    SeriesTmdb,
    SeriesTvdb,
}

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

pub fn provider_item(
    item: &MediaItem,
    recommendation_policy: RecommendationPolicy,
) -> ProviderItem {
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

    let (portable_reference_candidates, recommended_mapping_roots) =
        portable_references(item, recommendation_policy);
    ProviderItem {
        key: Some(item_key(&item.rating_key)),
        kind: Some(term(MEDIA_NAMESPACE, item.media_type.clone())),
        display_name: display_name(item),
        attributes,
        portable_reference_candidates,
        assets,
        recommended_mapping_roots,
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

fn portable_references(
    item: &MediaItem,
    recommendation_policy: RecommendationPolicy,
) -> (Vec<PortableReference>, Vec<PortableReference>) {
    let primary_reference = item
        .guid
        .as_deref()
        .and_then(|guid| portable_reference(guid, &item.media_type));
    let mut seen = HashSet::new();
    let portable_reference_candidates = primary_reference
        .into_iter()
        .chain(
            item.guids
                .iter()
                .filter_map(|guid| portable_reference(&guid.id, &item.media_type)),
        )
        .filter(|reference| seen.insert((reference.namespace.clone(), reference.value.clone())))
        .collect::<Vec<_>>();
    let recommended_mapping_roots = portable_reference_candidates
        .iter()
        .filter(|reference| recommendation_policy.recommends(reference))
        .cloned()
        .collect();
    (portable_reference_candidates, recommended_mapping_roots)
}

impl RecommendationPolicy {
    fn recommends(self, reference: &PortableReference) -> bool {
        match self {
            Self::None => false,
            Self::Movie => matches!(
                reference.namespace.as_str(),
                IMDB_REFERENCE_NAMESPACE | TMDB_REFERENCE_NAMESPACE | TVDB_REFERENCE_NAMESPACE
            ),
            Self::SeriesTmdb => reference.namespace == TMDB_REFERENCE_NAMESPACE,
            Self::SeriesTvdb => reference.namespace == TVDB_REFERENCE_NAMESPACE,
        }
    }
}

pub fn portable_reference(guid: &str, media_type: &str) -> Option<PortableReference> {
    let (scheme, identifier) = guid.split_once("://")?;
    if identifier.is_empty() {
        return None;
    }
    let scheme = scheme.to_ascii_lowercase();
    let (namespace, value) = match scheme.as_str() {
        "plex" => (PLEX_REFERENCE_NAMESPACE.to_owned(), identifier.to_owned()),
        "imdb" => (
            IMDB_REFERENCE_NAMESPACE.to_owned(),
            format!("{}/{}", imdb_resource(identifier)?, identifier),
        ),
        "tmdb" => (
            TMDB_REFERENCE_NAMESPACE.to_owned(),
            format!("{}/{}", tmdb_resource(media_type)?, identifier),
        ),
        "tvdb" => (
            TVDB_REFERENCE_NAMESPACE.to_owned(),
            format!("{}/{}", tvdb_resource(media_type)?, identifier),
        ),
        "mbid" => (
            MUSICBRAINZ_REFERENCE_NAMESPACE.to_owned(),
            format!("{}/{}", musicbrainz_resource(media_type)?, identifier),
        ),
        _ if reverse_dns_guid_scheme(&scheme) => (scheme, identifier.to_owned()),
        _ => return None,
    };
    Some(PortableReference {
        namespace,
        value: value.as_bytes().to_vec(),
    })
}

pub fn plex_guid(reference: &PortableReference) -> Option<String> {
    let value = std::str::from_utf8(&reference.value).ok()?;
    if value.is_empty() {
        return None;
    }
    if reference.namespace == PLEX_REFERENCE_NAMESPACE {
        return Some(format!("plex://{value}"));
    }

    let (resource, identifier) = value.split_once('/')?;
    if identifier.is_empty() {
        return None;
    }
    let scheme = match reference.namespace.as_str() {
        IMDB_REFERENCE_NAMESPACE if imdb_resource(identifier) == Some(resource) => "imdb",
        TMDB_REFERENCE_NAMESPACE
            if matches!(
                resource,
                "movie" | "tv" | "tv-season" | "tv-episode" | "person"
            ) =>
        {
            "tmdb"
        }
        TVDB_REFERENCE_NAMESPACE
            if matches!(
                resource,
                "movies" | "series" | "seasons" | "episodes" | "people"
            ) =>
        {
            "tvdb"
        }
        MUSICBRAINZ_REFERENCE_NAMESPACE
            if matches!(resource, "artist" | "release-group" | "recording") =>
        {
            "mbid"
        }
        _ => return None,
    };
    Some(format!("{scheme}://{identifier}"))
}

fn imdb_resource(identifier: &str) -> Option<&'static str> {
    match identifier.get(..2)? {
        "tt" => Some("title"),
        "nm" => Some("name"),
        "co" => Some("company"),
        _ => None,
    }
}

fn tmdb_resource(media_type: &str) -> Option<&'static str> {
    match media_type {
        "movie" => Some("movie"),
        "show" => Some("tv"),
        "season" => Some("tv-season"),
        "episode" => Some("tv-episode"),
        "person" => Some("person"),
        _ => None,
    }
}

fn tvdb_resource(media_type: &str) -> Option<&'static str> {
    match media_type {
        "movie" => Some("movies"),
        "show" => Some("series"),
        "season" => Some("seasons"),
        "episode" => Some("episodes"),
        "person" => Some("people"),
        _ => None,
    }
}

fn reverse_dns_guid_scheme(scheme: &str) -> bool {
    scheme.contains('.')
        && scheme.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn musicbrainz_resource(media_type: &str) -> Option<&'static str> {
    match media_type {
        "artist" => Some("artist"),
        "album" => Some("release-group"),
        "track" => Some("recording"),
        _ => None,
    }
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
            show_ordering: None,
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
        let item = provider_item(&episode, RecommendationPolicy::None);
        assert_eq!(item.display_name, "A Show - Season 1 - Pilot");
        assert_eq!(item.portable_reference_candidates.len(), 2);
        assert_eq!(
            item.portable_reference_candidates[0].namespace,
            PLEX_REFERENCE_NAMESPACE
        );
        assert_eq!(item.portable_reference_candidates[0].value, b"episode/abc");
        assert_eq!(
            item.portable_reference_candidates[1].namespace,
            IMDB_REFERENCE_NAMESPACE
        );
        assert_eq!(item.portable_reference_candidates[1].value, b"title/tt123");
        assert!(item.recommended_mapping_roots.is_empty());
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
    fn recommends_existing_candidates_for_the_section_policy() {
        let mut episode = episode();
        episode.guids = vec![
            PlexGuid {
                id: "tmdb://456".to_owned(),
            },
            PlexGuid {
                id: "tvdb://789".to_owned(),
            },
            PlexGuid {
                id: "imdb://tt123".to_owned(),
            },
            PlexGuid {
                id: "plex://episode/abc".to_owned(),
            },
            PlexGuid {
                id: "tmdb://456".to_owned(),
            },
            PlexGuid {
                id: "unknown://ignored".to_owned(),
            },
        ];

        let item = provider_item(&episode, RecommendationPolicy::Movie);
        assert_eq!(item.portable_reference_candidates.len(), 4);
        assert_eq!(
            item.recommended_mapping_roots,
            item.portable_reference_candidates[1..].to_vec()
        );

        let item = provider_item(&episode, RecommendationPolicy::SeriesTmdb);
        assert_eq!(
            item.recommended_mapping_roots,
            vec![item.portable_reference_candidates[1].clone()]
        );

        let item = provider_item(&episode, RecommendationPolicy::SeriesTvdb);
        assert_eq!(
            item.recommended_mapping_roots,
            vec![item.portable_reference_candidates[2].clone()]
        );

        let item = provider_item(&episode, RecommendationPolicy::None);
        assert!(item.recommended_mapping_roots.is_empty());
    }

    #[test]
    fn canonicalizes_database_namespaces_and_resource_paths() {
        let cases = [
            ("tvdb://123", "show", TVDB_REFERENCE_NAMESPACE, "series/123"),
            (
                "tvdb://456",
                "movie",
                TVDB_REFERENCE_NAMESPACE,
                "movies/456",
            ),
            ("tmdb://123", "show", TMDB_REFERENCE_NAMESPACE, "tv/123"),
            ("tmdb://456", "movie", TMDB_REFERENCE_NAMESPACE, "movie/456"),
            (
                "tmdb://789",
                "episode",
                TMDB_REFERENCE_NAMESPACE,
                "tv-episode/789",
            ),
            (
                "tvdb://789",
                "episode",
                TVDB_REFERENCE_NAMESPACE,
                "episodes/789",
            ),
            (
                "imdb://nm0000001",
                "person",
                IMDB_REFERENCE_NAMESPACE,
                "name/nm0000001",
            ),
            (
                "mbid://artist-id",
                "artist",
                MUSICBRAINZ_REFERENCE_NAMESPACE,
                "artist/artist-id",
            ),
            (
                "mbid://album-id",
                "album",
                MUSICBRAINZ_REFERENCE_NAMESPACE,
                "release-group/album-id",
            ),
            (
                "mbid://track-id",
                "track",
                MUSICBRAINZ_REFERENCE_NAMESPACE,
                "recording/track-id",
            ),
        ];

        for (guid, media_type, namespace, value) in cases {
            let reference = portable_reference(guid, media_type).expect("portable reference");
            assert_eq!(reference.namespace, namespace);
            assert_eq!(reference.value, value.as_bytes());
            assert_eq!(plex_guid(&reference).as_deref(), Some(guid));
        }
    }

    #[test]
    fn rejects_unknown_or_mismatched_portable_references() {
        assert!(portable_reference("unknown://123", "movie").is_none());
        assert!(
            plex_guid(&PortableReference {
                namespace: IMDB_REFERENCE_NAMESPACE.to_owned(),
                value: b"name/tt123".to_vec(),
            })
            .is_none()
        );
    }

    #[test]
    fn retains_reverse_dns_guid_candidates() {
        let guid = "org.example.agent://series/123";
        let reference = portable_reference(guid, "show").expect("custom agent reference");

        assert_eq!(reference.namespace, "org.example.agent");
        assert_eq!(reference.value, b"series/123");
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

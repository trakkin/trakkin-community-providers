use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaContainerEnvelope<T> {
    #[serde(rename = "MediaContainer")]
    pub media_container: T,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub machine_identifier: String,
    #[serde(default)]
    pub friendly_name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlexAccount {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub email: String,
}

impl PlexAccount {
    pub fn display_name(&self) -> &str {
        if self.username.is_empty() {
            &self.email
        } else {
            &self.username
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LibrarySections {
    #[serde(rename = "Directory", default)]
    pub directories: Vec<LibrarySection>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LibrarySection {
    pub key: String,
    #[serde(default)]
    pub uuid: String,
    pub title: String,
    #[serde(rename = "type")]
    pub media_type: String,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataContainer {
    #[serde(default)]
    pub size: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub total_size: Option<usize>,
    #[serde(rename = "Metadata", default)]
    pub metadata: Vec<MediaItem>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaItem {
    pub rating_key: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub parent_rating_key: Option<String>,
    #[serde(default)]
    pub grandparent_rating_key: Option<String>,
    #[serde(default)]
    pub guid: Option<String>,
    #[serde(rename = "type")]
    pub media_type: String,
    pub title: String,
    #[serde(default)]
    pub parent_title: Option<String>,
    #[serde(default)]
    pub grandparent_title: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub thumb: Option<String>,
    #[serde(default)]
    pub art: Option<String>,
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(default)]
    pub added_at: Option<i64>,
    #[serde(default)]
    pub updated_at: Option<i64>,
    #[serde(default)]
    pub originally_available_at: Option<String>,
    #[serde(default)]
    pub year: Option<i64>,
    #[serde(default)]
    pub index: Option<i64>,
    #[serde(default)]
    pub parent_index: Option<i64>,
    #[serde(default)]
    pub user_rating: Option<f64>,
    #[serde(default)]
    pub view_count: Option<i64>,
    #[serde(default)]
    pub view_offset: Option<i64>,
    #[serde(default)]
    pub last_viewed_at: Option<i64>,
    #[serde(rename = "Guid", default)]
    pub guids: Vec<PlexGuid>,
    #[serde(rename = "Genre", default)]
    pub genres: Vec<Tag>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PlexGuid {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Tag {
    pub tag: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pin {
    pub id: u64,
    pub code: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub expires_in: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub name: String,
    pub client_identifier: String,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub provides: String,
    #[serde(default)]
    pub connections: Vec<ResourceConnection>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConnection {
    pub uri: String,
    #[serde(default)]
    pub local: bool,
    #[serde(default)]
    pub relay: bool,
}

#[derive(Debug, Serialize)]
pub struct CreatePinRequest {
    pub strong: bool,
}

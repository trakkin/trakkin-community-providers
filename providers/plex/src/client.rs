use std::time::Duration;

use reqwest::{Method, StatusCode, redirect::Policy};
use serde::de::DeserializeOwned;
use url::Url;

use crate::{PRODUCT_NAME, model::MediaContainerEnvelope};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct PlexClient {
    http: reqwest::Client,
    public_http: reqwest::Client,
    base_url: Url,
    token: Option<String>,
    client_identifier: String,
}

impl PlexClient {
    pub fn new(
        base_url: &str,
        token: Option<String>,
        client_identifier: impl Into<String>,
    ) -> Result<Self, PlexError> {
        let base_url = normalize_base_url(base_url)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .build()
            .map_err(PlexError::Transport)?;
        let public_http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .build()
            .map_err(PlexError::Transport)?;
        Ok(Self {
            http,
            public_http,
            base_url,
            token,
            client_identifier: client_identifier.into(),
        })
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub async fn get_container<T: DeserializeOwned>(&self, path: &str) -> Result<T, PlexError> {
        self.get_container_with_query(path, &[]).await
    }

    pub async fn get_container_with_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, PlexError> {
        let envelope = self
            .request(Method::GET, path)?
            .query(query)
            .send()
            .await
            .map_err(PlexError::Transport)?
            .error_for_status()
            .map_err(PlexError::Response)?
            .json::<MediaContainerEnvelope<T>>()
            .await
            .map_err(PlexError::Transport)?;
        Ok(envelope.media_container)
    }

    pub async fn get_container_page<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        start: usize,
        size: usize,
    ) -> Result<T, PlexError> {
        let envelope = self
            .request(Method::GET, path)?
            .query(query)
            .header("X-Plex-Container-Start", start)
            .header("X-Plex-Container-Size", size)
            .send()
            .await
            .map_err(PlexError::Transport)?
            .error_for_status()
            .map_err(PlexError::Response)?
            .json::<MediaContainerEnvelope<T>>()
            .await
            .map_err(PlexError::Transport)?;
        Ok(envelope.media_container)
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, PlexError> {
        self.request_json(Method::GET, path, &[]).await
    }

    pub async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, PlexError> {
        self.request(method, path)?
            .query(query)
            .send()
            .await
            .map_err(PlexError::Transport)?
            .error_for_status()
            .map_err(PlexError::Response)?
            .json::<T>()
            .await
            .map_err(PlexError::Transport)
    }

    pub async fn send(&self, method: Method, path: &str) -> Result<(), PlexError> {
        self.send_with_query(method, path, &[]).await
    }

    pub async fn send_with_query(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<(), PlexError> {
        self.request(method, path)?
            .query(query)
            .send()
            .await
            .map_err(PlexError::Transport)?
            .error_for_status()
            .map_err(PlexError::Response)?;
        Ok(())
    }

    pub async fn get_bytes(
        &self,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<(Vec<u8>, String), PlexError> {
        let request = match public_asset_url(path)? {
            Some(url) => self
                .public_http
                .get(url)
                .header(reqwest::header::ACCEPT, "image/*"),
            None => self.request(Method::GET, path)?,
        };
        let mut response = request
            .send()
            .await
            .map_err(PlexError::Transport)?
            .error_for_status()
            .map_err(PlexError::Response)?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .unwrap_or("application/octet-stream")
            .to_owned();
        if response
            .content_length()
            .is_some_and(|length| length > maximum_bytes)
        {
            return Err(PlexError::AssetTooLarge);
        }
        let mut content = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(PlexError::Transport)? {
            if content.len().saturating_add(chunk.len()) as u64 > maximum_bytes {
                return Err(PlexError::AssetTooLarge);
            }
            content.extend_from_slice(&chunk);
        }
        Ok((content, content_type))
    }

    fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder, PlexError> {
        let mut url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|_| PlexError::InvalidPath)?;
        if url.origin() != self.base_url.origin() {
            return Err(PlexError::InvalidPath);
        }
        remove_plex_token(&mut url);
        let mut request = self
            .http
            .request(method, url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header("X-Plex-Client-Identifier", &self.client_identifier)
            .header("X-Plex-Product", PRODUCT_NAME)
            .header("X-Plex-Version", env!("CARGO_PKG_VERSION"));
        if let Some(token) = &self.token {
            request = request.header("X-Plex-Token", token);
        }
        Ok(request)
    }
}

fn public_asset_url(path: &str) -> Result<Option<Url>, PlexError> {
    let Ok(mut url) = Url::parse(path) else {
        return Ok(None);
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(PlexError::InvalidAssetUrl);
    }
    remove_plex_token(&mut url);
    Ok(Some(url))
}

fn remove_plex_token(url: &mut Url) {
    let query = url
        .query_pairs()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("X-Plex-Token"))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query);
    }
}

pub fn normalize_base_url(value: &str) -> Result<Url, PlexError> {
    let mut url = Url::parse(value).map_err(|_| PlexError::InvalidBaseUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(PlexError::InvalidBaseUrl);
    }
    url.set_path(&format!("{}/", url.path().trim_end_matches('/')));
    Ok(url)
}

#[derive(Debug, thiserror::Error)]
pub enum PlexError {
    #[error("Plex server URL must be an HTTP(S) origin without credentials, query, or fragment")]
    InvalidBaseUrl,
    #[error("Plex returned a request path outside the configured server origin")]
    InvalidPath,
    #[error("Plex returned an invalid public asset URL")]
    InvalidAssetUrl,
    #[error("Plex request failed")]
    Transport(#[source] reqwest::Error),
    #[error("Plex returned HTTP {0}")]
    Response(#[source] reqwest::Error),
    #[error("Plex asset exceeds the requested size limit")]
    AssetTooLarge,
    #[error("Plex item does not expose the requested asset")]
    AssetNotFound,
}

impl PlexError {
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Transport(error) | Self::Response(error) => error.status(),
            Self::InvalidBaseUrl
            | Self::InvalidPath
            | Self::InvalidAssetUrl
            | Self::AssetTooLarge
            | Self::AssetNotFound => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PlexClient, PlexError, normalize_base_url};

    #[test]
    fn normalizes_a_server_origin() {
        assert_eq!(
            normalize_base_url("http://plex.local:32400")
                .unwrap()
                .as_str(),
            "http://plex.local:32400/"
        );
    }

    #[test]
    fn rejects_credentials_and_non_http_schemes() {
        assert!(normalize_base_url("http://token@plex.local:32400").is_err());
        assert!(normalize_base_url("file:///tmp/plex").is_err());
    }

    #[test]
    fn authenticated_requests_cannot_cross_origins() {
        let client = PlexClient::new(
            "http://plex.local:32400",
            Some("secret".to_owned()),
            "test-client",
        )
        .unwrap();
        assert!(matches!(
            client.request(reqwest::Method::GET, "https://example.com/image.jpg"),
            Err(PlexError::InvalidPath)
        ));
    }
}

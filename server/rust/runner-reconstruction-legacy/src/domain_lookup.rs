use anyhow::Result;
use posemesh_domain_http::domain_data::{download_metadata_v1, DomainDataMetadata, DownloadQuery};

/// Resolve an existing Domain data object by (name, data_type), returning its ID if found.
pub async fn resolve_domain_data_id(
    domain_url: &str,
    client_id: &str,
    token: &str,
    domain_id: &str,
    name: &str,
    data_type: &str,
) -> Result<Option<String>> {
    // Query metadata filtered by name and data_type. Empty ids list to avoid narrowing by IDs.
    let metas: Vec<DomainDataMetadata> = download_metadata_v1(
        domain_url,
        client_id,
        token,
        domain_id,
        &DownloadQuery {
            ids: Vec::new(),
            name: Some(name.to_string()),
            data_type: Some(data_type.to_string()),
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("download metadata failed: {}", e))?;

    Ok(metas
        .into_iter()
        .find(|m| m.name == name && m.data_type == data_type)
        .map(|m| m.id))
}

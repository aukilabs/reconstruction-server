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
    // `posemesh-domain-http` will append `/api/v1/...` to `domain_url`, so ensure we don't
    // accidentally generate `//api/v1/...` when the base URL has a trailing slash.
    let domain_url = domain_url.trim().trim_end_matches('/');

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

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};
    use std::convert::Infallible;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn resolve_domain_data_id_trims_trailing_slash() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let service = make_service_fn(|_| async move {
            Ok::<_, Infallible>(service_fn(|req: Request<Body>| async move {
                if req.uri().path() != "/api/v1/domains/dom1/data" {
                    return Ok::<_, Infallible>(
                        Response::builder()
                            .status(404)
                            .body(Body::from("not found"))
                            .unwrap(),
                    );
                }

                let body = r#"{"data":[{"id":"id123","domain_id":"dom1","name":"refined_scan_foo","data_type":"refined_scan_zip","size":1,"created_at":"2020-01-01T00:00:00Z","updated_at":"2020-01-01T00:00:00Z"}]}"#;
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
            }))
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = Server::from_tcp(listener).unwrap().serve(service);
        let handle = tokio::spawn(server.with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        }));

        let base = format!("http://{}/", addr); // trailing slash should be handled
        let resolved = resolve_domain_data_id(
            &base,
            "client-1",
            "token-1",
            "dom1",
            "refined_scan_foo",
            "refined_scan_zip",
        )
        .await
        .unwrap();
        assert_eq!(resolved.as_deref(), Some("id123"));

        let _ = shutdown_tx.send(());
        let _ = handle.await;
    }
}

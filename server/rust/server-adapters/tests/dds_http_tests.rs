use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use server_adapters::dds::{
    http::{router_dds, DdsState},
    persist::read_node_secret_from_path,
};
use std::path::PathBuf;
use tower::ServiceExt;

#[tokio::test]
async fn callback_persists_and_redacts_secret() {
    // Set up log capture
    use parking_lot::Mutex as PLMutex;
    use std::io;
    use std::sync::Arc;
    use tracing::subscriber;
    use tracing_subscriber::layer::SubscriberExt;

    struct BufWriter(Arc<PLMutex<Vec<u8>>>);
    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    struct MakeBufWriter(Arc<PLMutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeBufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            BufWriter(self.0.clone())
        }
    }

    let buf = Arc::new(PLMutex::new(Vec::<u8>::new()));
    let make = MakeBufWriter(buf.clone());
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(make)
        .with_ansi(false)
        .without_time();
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = subscriber::set_default(subscriber);

    // Prepare router with temp secret path
    let secret_path = PathBuf::from(format!("dds_http_test/{}", uuid::Uuid::new_v4()));
    let app = router_dds(DdsState {
        secret_path: secret_path.clone(),
    });

    // Build request
    let secret = "my-very-secret";
    let body = serde_json::json!({
        "id": "abc123",
        "secret": secret,
        "organization_id": "org1",
        "lighthouses_in_domains": [],
        "domains": []
    })
    .to_string();

    let req = Request::builder()
        .method("POST")
        .uri("/internal/v1/registrations")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();

    // Exercise handler
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Secret stored in memory and retrievable via persist API
    let got = read_node_secret_from_path(&secret_path).unwrap();
    assert_eq!(got.as_deref(), Some(secret));

    // Check logs do not include the secret
    let captured = String::from_utf8(buf.lock().clone()).unwrap_or_default();
    assert!(captured.contains("Received registration callback"));
    assert!(
        !captured.contains(secret),
        "logs leaked secret: {}",
        captured
    );
}

#[tokio::test]
async fn health_ok() {
    let secret_path = PathBuf::from(format!("dds_http_test/{}", uuid::Uuid::new_v4()));
    let app = router_dds(DdsState { secret_path });

    let req = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

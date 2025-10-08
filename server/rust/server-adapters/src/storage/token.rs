use async_trait::async_trait;

#[async_trait]
pub trait DomainTokenProvider: Send + Sync {
    async fn bearer(&self) -> Option<String>;
}

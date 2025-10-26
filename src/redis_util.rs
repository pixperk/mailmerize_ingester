use redis::{aio::ConnectionManager, AsyncCommands, RedisError};
use std::env;


pub async fn create_redis_client() -> Result<ConnectionManager, RedisError> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    log::info!("Connecting to Redis at {}...", redis_url);

    let client = redis::Client::open(redis_url)?;
    let manager = ConnectionManager::new(client).await?;

    log::info!("Successfully connected to Redis");

    Ok(manager)
}


pub async fn is_message_processed(
    conn: &mut ConnectionManager,
    account: &str,
    uid: u32,
) -> Result<bool, RedisError> {
    let key = format!("processed_emails:{}", account);
    conn.sismember(&key, uid).await
}


pub async fn mark_message_processed(
    conn: &mut ConnectionManager,
    account: &str,
    uid: u32,
) -> Result<(), RedisError> {
    let key = format!("processed_emails:{}", account);
    conn.sadd(&key, uid).await
}


pub async fn get_processed_uids(
    conn: &mut ConnectionManager,
    account: &str,
) -> Result<Vec<u32>, RedisError> {
    let key = format!("processed_emails:{}", account);
    conn.smembers(&key).await
}


pub async fn clear_processed_uids(
    conn: &mut ConnectionManager,
    account: &str,
) -> Result<(), RedisError> {
    let key = format!("processed_emails:{}", account);
    conn.del(&key).await
}


pub async fn get_processed_count(
    conn: &mut ConnectionManager,
    account: &str,
) -> Result<usize, RedisError> {
    let key = format!("processed_emails:{}", account);
    conn.scard(&key).await
}

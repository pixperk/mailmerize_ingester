use std::env;
use std::sync::Arc;

use async_native_tls::TlsConnector;
use chrono::Datelike;
use chrono::{DateTime, Duration, Utc};
use lapin::{Channel, Connection, ConnectionProperties, BasicProperties};
use lapin::options::BasicPublishOptions;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use async_imap::{Client, types::Uid};
use futures_util::future::join_all;
use futures_util::stream::TryStreamExt;
use mailparse::{parse_mail, MailHeaderMap};

mod redis_util;

//read from accounts.json for now
const ACCOUNTS_JSON: &str = include_str!("../accounts.json");

#[derive(Deserialize, Debug, Clone)]
enum FetchSince {
    Days(u32),
}

#[derive(Deserialize, Debug, Clone)]
struct AccountConfig {
    imap_domain: String,
    imap_user: String,
    tls_port: Option<u16>,
    imap_pass: String,
    fetch_since: Option<FetchSince>,
}

#[derive(Deserialize, Debug, Clone)]
struct UserConfig {
    user_id: String,
    user_name: String,
    accounts: Vec<AccountConfig>,
}

#[derive(Serialize, Debug)]
struct EmailHeaders {
    subject: Option<String>,
    from: Option<String>,
    to: Vec<String>,
    cc: Vec<String>,
    date: Option<String>,
    message_id: Option<String>,
}

#[derive(Serialize, Debug)]
struct EmailBody {
    text: Option<String>,
    html: Option<String>,
}

#[derive(Serialize, Debug)]
struct AttachmentInfo {
    filename: String,
    content_type: String,
    size: usize,
}

#[derive(Serialize, Debug)]
struct EmailMessage {
    uid: u32,
    user_id: String,
    account: String,
    ingested_at: DateTime<Utc>,
    headers: EmailHeaders,
    body: EmailBody,
    has_attachments: bool,
    attachments: Vec<AttachmentInfo>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    unsafe {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let channel = connect_rabbitmq().await?;
    let channel = Arc::new(channel);

    let redis_client = redis_util::create_redis_client().await?;
    let redis_client = Arc::new(redis_client);

    let users: Vec<UserConfig> = serde_json::from_str(ACCOUNTS_JSON)?;

    let mut tasks = Vec::new();

    for user in users {
        for account in user.accounts {
            let channel = channel.clone();
            let redis_client = redis_client.clone();
            let user_id = user.user_id.clone();

            tasks.push(tokio::spawn(async move {
                if let Err(e) = process_account(user_id.clone(), account.clone(), channel, redis_client).await {
                    log::error!("[{}][{}] Worker failed: {}", user_id, account.imap_user, e);
                }
            }));
        }
    }

    join_all(tasks).await;
    Ok(())
}

async fn process_account(
    user_id: String,
    config: AccountConfig,
    channel: Arc<Channel>,
    redis_client: Arc<ConnectionManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::info!("[{}][{}] Starting worker", user_id, config.imap_user);

    let user = &config.imap_user;
    let domain = &config.imap_domain;

    let port = config.tls_port.unwrap_or(993);

    log::info!("[{}] Connecting to IMAP at {}:{}...", user, domain, port);

    let tcp_stream = TcpStream::connect((domain.as_str(), port)).await?;

    let tcp_stream = tcp_stream.compat();

    let tls = TlsConnector::new();
    let tls_stream = tls.connect(domain, tcp_stream).await?;

    let mut client = Client::new(tls_stream);
    client.read_response().await?;

    log::info!("[{}] Authenticating...", user);

    let mut session = client
        .login(&config.imap_user, &config.imap_pass)
        .await
        .map_err(|e| e.0)?;

    log::info!("[{}] Successfully connected and authenticated", user);

    session.select("INBOX").await?;

    loop {
        let query = match &config.fetch_since {
            Some(FetchSince::Days(days)) => {
                let date = Utc::now().date_naive() - Duration::days(*days as i64);

                let formatted = format!("{}-{}-{}", date.day(), date.format("%b"), date.year());
                format!("UNSEEN SINCE {}", formatted)
            }
            None => "UNSEEN".to_string(),
        };
        let all_uids: Vec<Uid> = session.uid_search(&query).await?.into_iter().collect();

       
        let mut uids = Vec::new();
        let mut redis_conn = redis_client.as_ref().clone();
        for uid in all_uids {
            match redis_util::is_message_processed(&mut redis_conn, &user_id, user, uid).await {
                Ok(false) => uids.push(uid),
                Ok(true) => {
                    log::debug!("[{}][{}] UID {} already processed, skipping", user_id, user, uid);
                }
                Err(e) => {
                    log::error!("[{}][{}] Redis error checking UID {}: {}", user_id, user, uid, e);
                    uids.push(uid);
                }
            }
        }

        if uids.is_empty() {
            log::info!("[{}] No new messages. Entering IDLE mode...", user);

            let mut idle = session.idle();
            idle.init().await?;

            let (idle_wait, _interrupt) =
                idle.wait_with_timeout(std::time::Duration::from_secs(300));

            idle_wait.await?;

            session = idle.done().await?;

            log::info!("[{}] IDLE completed. Checking for new messages...", user);
        } else {
            log::info!(
                "[{}] Found {} unseen messages. Fetching...",
                user,
                uids.len()
            );
            let sequence_set = uids
                .iter()
                .map(|uid| uid.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let message_stream = session
                .uid_fetch(sequence_set.clone(), "(UID BODY.PEEK[])")
                .await?;

            let mut processed_uids = Vec::new();
            {
                let mut message_stream = std::pin::pin!(message_stream);

                while let Some(msg) = message_stream.try_next().await? {
                    if let Some(uid) = msg.uid {
                        let body_bytes = msg.body().unwrap_or(&[]);

                        match parse_and_publish_email(uid, body_bytes, &user_id, user, &channel).await {
                            Ok(_) => {
                                log::info!("[{}][{}] Ingested UID {}", user_id, user, uid);


                                let mut redis_conn = redis_client.as_ref().clone();
                                if let Err(e) = redis_util::mark_message_processed(&mut redis_conn, &user_id, user, uid).await {
                                    log::error!("[{}][{}] Failed to mark UID {} as processed in Redis: {}", user_id, user, uid, e);
                                }

                                processed_uids.push(uid);
                            }
                            Err(e) => {
                                log::error!("[{}][{}] Failed to process UID {}: {}", user_id, user, uid, e);
                            }
                        }
                    }
                }
            }

            log::info!("[{}] Processed {} messages", user, processed_uids.len());
        }
    }
}

async fn connect_rabbitmq() -> Result<Channel, lapin::Error> {
    let amqp_addr =
        env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());

    log::info!("Connecting to RabbitMQ at {}...", amqp_addr);
    let conn = Connection::connect(&amqp_addr, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let queue = env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "email_tasks".into());

let _queue = channel
    .queue_declare(
        &queue,
        lapin::options::QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        lapin::types::FieldTable::default(),
    )
    .await?;

    Ok(channel)
}

async fn parse_and_publish_email(
    uid: u32,
    email_bytes: &[u8],
    user_id: &str,
    account: &str,
    channel: &Channel,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let parsed = parse_mail(email_bytes)?;

    // Extract headers
    let headers = EmailHeaders {
        subject: parsed.headers.get_first_value("Subject"),
        from: parsed.headers.get_first_value("From"),
        to: parsed
            .headers
            .get_all_values("To")
            .into_iter()
            .collect(),
        cc: parsed
            .headers
            .get_all_values("Cc")
            .into_iter()
            .collect(),
        date: parsed.headers.get_first_value("Date"),
        message_id: parsed.headers.get_first_value("Message-ID"),
    };

    // Extract body parts
    let mut body_text = None;
    let mut body_html = None;
    let mut attachments = Vec::new();

    fn extract_parts(
        part: &mailparse::ParsedMail,
        text: &mut Option<String>,
        html: &mut Option<String>,
        attachments: &mut Vec<AttachmentInfo>,
    ) {
        let content_type = part.ctype.mimetype.as_str();

        match content_type {
            "text/plain" if part.get_content_disposition().disposition == mailparse::DispositionType::Inline => {
                if text.is_none() {
                    *text = part.get_body().ok();
                }
            }
            "text/html" if part.get_content_disposition().disposition == mailparse::DispositionType::Inline => {
                if html.is_none() {
                    *html = part.get_body().ok();
                }
            }
            _ => {
                let disposition = part.get_content_disposition();
                if disposition.disposition == mailparse::DispositionType::Attachment {
                    let filename = disposition
                        .params
                        .get("filename")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());

                    attachments.push(AttachmentInfo {
                        filename,
                        content_type: content_type.to_string(),
                        size: part.get_body_raw().map(|b| b.len()).unwrap_or(0),
                    });
                }
            }
        }

        for subpart in &part.subparts {
            extract_parts(subpart, text, html, attachments);
        }
    }

    extract_parts(&parsed, &mut body_text, &mut body_html, &mut attachments);

    let email_message = EmailMessage {
        uid,
        user_id: user_id.to_string(),
        account: account.to_string(),
        ingested_at: Utc::now(),
        headers,
        body: EmailBody {
            text: body_text,
            html: body_html,
        },
        has_attachments: !attachments.is_empty(),
        attachments,
    };

    // Publish to RabbitMQ
    let queue = env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "email_tasks".into());
    let payload = serde_json::to_vec(&email_message)?;

    channel
        .basic_publish(
            "",
            &queue,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await?;

    Ok(())
}

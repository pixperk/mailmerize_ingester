use std::env;

use async_native_tls::TlsConnector;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use async_imap::{Client, types::Uid};
use futures_util::future::join_all;
use futures_util::stream::TryStreamExt;

//read from accounts.json for now
const ACCOUNTS_JSON: &str = include_str!("../accounts.json");

#[derive(Deserialize, Debug, Clone)]
struct AccountConfig {
    imap_domain: String,
    imap_user: String,
    tls_port: Option<u16>,
    imap_pass: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    //setup amqp connection [later]

    let accounts: Vec<AccountConfig> = serde_json::from_str(ACCOUNTS_JSON)?;

    let mut tasks = Vec::new();

    for account in accounts {
        tasks.push(tokio::spawn(async move {
            if let Err(e) = process_account(account.clone()).await {
                log::error!("[{}] Worker failed: {}", account.imap_user, e);
            }
        }));
    }

    join_all(tasks).await;
    Ok(())
}

async fn process_account(config: AccountConfig) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("[{}] Starting worker", config.imap_user);

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
        let uids: Vec<Uid> = session.uid_search("UNSEEN").await?.into_iter().collect();

        if uids.is_empty() {
            log::info!("[{}] No unseen messages. Starting IDLE...", user);

            let mut idle = session.idle();
            idle.init().await?;

            // Wait for up to 29 minutes (servers typically disconnect after 30 minutes)
            let (wait_fut, _) = idle.wait_with_timeout(std::time::Duration::from_secs(29 * 60));
            match wait_fut.await {
                Ok(response) => {
                    log::debug!("[{}] Got IMAP update: {:?}", user, response);
                    //close idle
                    idle.done().await?;
                    break;
                }
                Err(e) => {
                    log::error!("[{}] Error in IDLE loop: {}", user, e);
                    return Err(e.into());
                }
            }
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
                .uid_fetch(sequence_set.clone(), "(UID BODY.PEEK[HEADER])")
                .await?;

            let mut processed_uids = Vec::new();
            {
                let mut message_stream = std::pin::pin!(message_stream);

                while let Some(msg) = message_stream.try_next().await? {
                    if let Some(uid) = msg.uid {
                        log::info!("[{}] Message UID: {}", user, uid);

                        let header_bytes = msg.header().or_else(|| msg.body());

                        if let Some(header_bytes) = header_bytes {
                            let header_str = String::from_utf8_lossy(header_bytes);
                            log::debug!("[{}] Raw header:\n{}", user, header_str);

                            let mut subject: Option<String> = None;
                            for line in header_str.lines() {
                                let trimmed = line.trim();
                                if trimmed.to_ascii_lowercase().starts_with("subject:") {
                                    subject = Some(trimmed["subject:".len()..].trim().to_string());
                                    break;
                                }
                            }

                            if let Some(subj) = subject {
                                log::info!("[{}] Subject: {}", user, subj);
                            } else {
                                log::warn!("[{}] No Subject header for UID {}", user, uid);
                            }

                            log::info!("[{}] {}", user, "=".repeat(80));
                        } else {
                            log::warn!("[{}] No header found for UID {}", user, uid);
                        }

                        processed_uids.push(uid);
                    }
                }
            }

            if !processed_uids.is_empty() {
                let uid_sequence = processed_uids
                    .iter()
                    .map(|uid| uid.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                let store_stream = session.uid_store(uid_sequence, "+FLAGS (\\Seen)").await?;
                let _store_responses: Vec<_> = store_stream.try_collect().await?;
            }

            log::info!("[{}] Processed {} messages", user, processed_uids.len());
        }
    }

    Ok(())
}

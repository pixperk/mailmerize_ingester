use std::env;

use async_native_tls::TlsConnector;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use futures_util::future::join_all;
use async_imap::Client;

//read from accounts.json
const ACCOUNTS_JSON: &str = include_str!("../accounts.json");



#[derive(Deserialize, Debug, Clone)]
struct AccountConfig {
    imap_domain: String,
    imap_user: String,
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
        tasks.push(tokio::spawn(async move{
            if let Err(e) = process_account(account.clone()).await{
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

    log::info!("[{}] Connecting to IMAP at {}:993...", user, domain);

   
    let tcp_stream = TcpStream::connect((domain.as_str(), 993)).await?;

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

    // TODO: Process emails

    session.logout().await?;
    log::info!("[{}] Logged out", user);

    Ok(())
}
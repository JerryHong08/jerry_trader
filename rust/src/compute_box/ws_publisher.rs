// rust/src/compute_box/ws_publisher.rs
//
// WebSocket Publisher for Rust Compute Box.
//
// Handles real-time bar and factor push to frontend subscribers.
// Uses tokio-tungstenite for async WebSocket server.
//
// See roadmap/rust-compute-box-architecture.md for full design.

use crate::bars::CompletedBar;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::tungstenite::Message;

// ── Message Types ───────────────────────────────────────────────────────

/// Incoming command from frontend.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action")]
pub enum WsCommand {
    #[serde(rename = "subscribe")]
    Subscribe {
        ticker: String,
        timeframes: Vec<String>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        ticker: String,
    },
}

/// Outgoing message to frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum WsMessage {
    /// Acknowledgment of subscribe request.
    #[serde(rename = "subscribe_ack")]
    SubscribeAck {
        ticker: String,
        timeframes: Vec<String>,
        status: String,       // "ready" or "bootstrapping"
        message: String,
    },

    /// Bootstrap completed successfully.
    #[serde(rename = "bootstrap_ready")]
    BootstrapReady {
        ticker: String,
        bars_count: HashMap<String, usize>,  // timeframe → count
        message: String,
    },

    /// Bootstrap failed.
    #[serde(rename = "bootstrap_failed")]
    BootstrapFailed {
        ticker: String,
        error: String,
        message: String,
    },

    /// Real-time bar update.
    #[serde(rename = "bar")]
    Bar {
        ticker: String,
        timeframe: String,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        trade_count: u64,
        vwap: f64,
        bar_start: i64,
        bar_end: i64,
        session: String,
    },

    /// Real-time factor update.
    #[serde(rename = "factor")]
    Factor {
        ticker: String,
        name: String,
        timeframe: String,
        value: f64,
        ts: i64,
    },
}

impl WsMessage {
    /// Convert to WebSocket text message.
    pub fn to_ws_message(&self) -> Message {
        let json = serde_json::to_string(self).unwrap_or_default();
        Message::Text(json.into())
    }

    /// Create a Bar message from CompletedBar.
    pub fn from_bar(bar: &CompletedBar) -> Self {
        WsMessage::Bar {
            ticker: bar.ticker.clone(),
            timeframe: bar.timeframe.to_string(),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            trade_count: bar.trade_count,
            vwap: bar.vwap,
            bar_start: bar.bar_start,
            bar_end: bar.bar_end,
            session: bar.session.to_string(),
        }
    }

    /// Create a SubscribeAck message.
    pub fn subscribe_ack(ticker: String, timeframes: Vec<String>, status: String) -> Self {
        let message = if status == "ready" {
            "Subscription confirmed"
        } else {
            "Bootstrap in progress..."
        };
        WsMessage::SubscribeAck {
            ticker,
            timeframes,
            status,
            message: message.to_string(),
        }
    }

    /// Create a BootstrapReady message.
    pub fn bootstrap_ready(ticker: String, bars_count: HashMap<String, usize>) -> Self {
        WsMessage::BootstrapReady {
            ticker,
            bars_count,
            message: "Bootstrap complete".to_string(),
        }
    }

    /// Create a BootstrapFailed message.
    pub fn bootstrap_failed(ticker: String, error: String) -> Self {
        WsMessage::BootstrapFailed {
            ticker,
            error,
            message: "Bootstrap failed".to_string(),
        }
    }
}

// ── Subscriber State ───────────────────────────────────────────────────

/// Subscription info for a single client.
#[derive(Debug, Clone)]
pub struct Subscription {
    pub ticker: String,
    pub timeframes: Vec<String>,
}

/// All active subscribers.
/// Key: ticker → list of (client_id, timeframes)
type SubscribersMap = Arc<RwLock<HashMap<String, Vec<(usize, Vec<String>)>>>>;

// ── WSPublisher ────────────────────────────────────────────────────────

/// WebSocket publisher server.
///
/// Runs on port 8000 and handles:
///   - subscribe/unsubscribe commands
///   - bar/factor broadcast to subscribers
///   - SubscribeAck/BootstrapReady protocol
pub struct WSPublisher {
    /// Port to listen on.
    port: u16,

    /// Broadcast channel for outgoing bars.
    bar_tx: broadcast::Sender<CompletedBar>,

    /// Channel for bootstrap status updates.
    bootstrap_tx: mpsc::Sender<(String, bool, Option<String>)>,  // (ticker, success, error_msg)

    /// Active subscribers (ticker → client subscriptions).
    subscribers: SubscribersMap,

    /// Client ID counter.
    next_client_id: Arc<RwLock<usize>>,
}

impl WSPublisher {
    /// Create a new WebSocket publisher.
    pub fn new(port: u16) -> Self {
        let (bar_tx, _) = broadcast::channel(256);
        let (bootstrap_tx, _) = mpsc::channel(64);
        let subscribers = Arc::new(RwLock::new(HashMap::new()));
        let next_client_id = Arc::new(RwLock::new(1));

        Self {
            port,
            bar_tx,
            bootstrap_tx,
            subscribers,
            next_client_id,
        }
    }

    /// Get a sender for broadcasting bars.
    pub fn bar_sender(&self) -> broadcast::Sender<CompletedBar> {
        self.bar_tx.clone()
    }

    /// Get a sender for bootstrap status updates.
    pub fn bootstrap_sender(&self) -> mpsc::Sender<(String, bool, Option<String>)> {
        self.bootstrap_tx.clone()
    }

    /// Start the WebSocket server.
    ///
    /// This spawns a tokio task that listens on the configured port
    /// and handles incoming WebSocket connections.
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse()?;
        let listener = TcpListener::bind(&addr).await?;

        log::info!("WSPublisher listening on {}", addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            log::debug!("New WebSocket connection from {}", peer_addr);

            let publisher = self.clone();
            tokio::spawn(async move {
                if let Err(e) = publisher.handle_connection(stream).await {
                    log::warn!("WebSocket connection error: {}", e);
                }
            });
        }
    }

    /// Handle a single WebSocket connection.
    async fn handle_connection(self: Arc<Self>, stream: tokio::net::TcpStream) -> anyhow::Result<()> {
        let ws_stream = tokio_tungstenite::accept_async(stream).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Assign client ID
        let client_id = {
            let mut id_guard = self.next_client_id.write().await;
            let id = *id_guard;
            *id_guard += 1;
            id
        };

        log::debug!("Client {} connected", client_id);

        // Track subscriptions for this client
        let client_subs: Arc<RwLock<Vec<Subscription>>> = Arc::new(RwLock::new(Vec::new()));

        // Subscribe to bar broadcast
        let mut bar_rx = self.bar_tx.subscribe();

        // Process incoming commands and outgoing messages
        loop {
            tokio::select! {
                // Incoming command
                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            // Parse and handle command inline to avoid mutable borrow conflict
                            if let Ok(cmd) = serde_json::from_str::<WsCommand>(&text) {
                                match cmd {
                                    WsCommand::Subscribe { ticker, timeframes } => {
                                        // Register subscription
                                        {
                                            let mut subs = self.subscribers.write().await;
                                            subs.entry(ticker.clone())
                                                .or_insert_with(Vec::new)
                                                .push((client_id, timeframes.clone()));
                                        }

                                        // Track in client subscriptions
                                        {
                                            let mut client = client_subs.write().await;
                                            client.push(Subscription {
                                                ticker: ticker.clone(),
                                                timeframes: timeframes.clone(),
                                            });
                                        }

                                        // Send SubscribeAck
                                        let ack = WsMessage::subscribe_ack(ticker.clone(), timeframes.clone(), "ready".to_string());
                                        ws_sender.send(ack.to_ws_message()).await?;

                                        log::debug!("Client {} subscribed to {} timeframes {:?}", client_id, ticker, timeframes);
                                    }

                                    WsCommand::Unsubscribe { ticker } => {
                                        self.unsubscribe_client(client_id, &ticker).await;

                                        // Remove from client subscriptions
                                        {
                                            let mut client = client_subs.write().await;
                                            client.retain(|s| s.ticker != ticker);
                                        }

                                        log::debug!("Client {} unsubscribed from {}", client_id, ticker);
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            log::debug!("Client {} disconnected", client_id);
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            ws_sender.send(Message::Pong(data)).await?;
                        }
                        None => break,
                        _ => {}
                    }
                }

                // Outgoing bar broadcast
                Ok(bar) = bar_rx.recv() => {
                    // Check if this client is subscribed to this ticker
                    let subs = client_subs.read().await;
                    for sub in subs.iter() {
                        if sub.ticker == bar.ticker && sub.timeframes.contains(&bar.timeframe.to_string()) {
                            let msg = WsMessage::from_bar(&bar).to_ws_message();
                            ws_sender.send(msg).await?;
                        }
                    }
                }
            }
        }

        // Cleanup: unsubscribe from all tickers
        let subs = client_subs.read().await.clone();
        for sub in subs {
            self.unsubscribe_client(client_id, &sub.ticker).await;
        }

        Ok(())
    }

    /// Remove a client's subscription for a ticker.
    async fn unsubscribe_client(&self, client_id: usize, ticker: &str) {
        let mut subs = self.subscribers.write().await;
        if let Some(entries) = subs.get_mut(ticker) {
            entries.retain(|(id, _)| *id != client_id);
            if entries.is_empty() {
                subs.remove(ticker);
            }
        }
    }

    /// Spawn the broadcast loop for bars and bootstrap messages.
    async fn spawn_broadcast_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // This would handle the internal broadcast channel
            // Currently a placeholder for the full implementation
            // which will integrate with BarBuilderEngine
            log::debug!("Broadcast loop started");
        })
    }
}

// ── Unit Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_message_serialize_bar() {
        let bar = CompletedBar {
            ticker: "AAPL".to_string(),
            timeframe: "1m",
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 102.0,
            volume: 5000.0,
            trade_count: 100,
            vwap: 101.0,
            bar_start: 1000,
            bar_end: 1060,
            session: "regular",
        };

        let msg = WsMessage::from_bar(&bar);
        let json = serde_json::to_string(&msg).unwrap();

        assert!(json.contains("\"type\":\"bar\""));
        assert!(json.contains("\"ticker\":\"AAPL\""));
        assert!(json.contains("\"timeframe\":\"1m\""));
        assert!(json.contains("\"close\":102"));
    }

    #[test]
    fn test_ws_message_subscribe_ack() {
        let ack = WsMessage::subscribe_ack(
            "BIAF".to_string(),
            vec!["10s".to_string(), "1m".to_string()],
            "ready".to_string(),
        );

        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"type\":\"subscribe_ack\""));
        assert!(json.contains("\"status\":\"ready\""));
    }

    #[test]
    fn test_ws_command_deserialize_subscribe() {
        let json = r#"{"action":"subscribe","ticker":"AAPL","timeframes":["1m","5m"]}"#;
        let cmd: WsCommand = serde_json::from_str(json).unwrap();

        match cmd {
            WsCommand::Subscribe { ticker, timeframes } => {
                assert_eq!(ticker, "AAPL");
                assert_eq!(timeframes, vec!["1m", "5m"]);
            }
            _ => panic!("Expected Subscribe command"),
        }
    }

    #[test]
    fn test_ws_command_deserialize_unsubscribe() {
        let json = r#"{"action":"unsubscribe","ticker":"AAPL"}"#;
        let cmd: WsCommand = serde_json::from_str(json).unwrap();

        match cmd {
            WsCommand::Unsubscribe { ticker } => {
                assert_eq!(ticker, "AAPL");
            }
            _ => panic!("Expected Unsubscribe command"),
        }
    }
}

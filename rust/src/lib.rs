use futures::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use uuid::Uuid;

// --- Inlined Domain Models for standalone dependency safety ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityType {
    Livetest,
    Backtest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategy {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeExecutedEvent {
    pub trade_id: Uuid,
    pub account_id: Uuid,
    pub strategy_id: Uuid,
    pub symbol: String,
    pub side: OrderSide,
    pub size: Decimal,
    pub price: Decimal,
    pub timestamp: i64,
    pub entry_price: Option<Decimal>,
    pub entry_time: Option<i64>,
    pub type_: OrderType,
    pub status: OrderStatus,
    pub pnl: Option<Decimal>,
    pub commission: Option<Decimal>,
    pub entry_id: Option<String>,
    pub exit_id: Option<String>,
    pub is_backtest: bool,
    pub strategy: Option<Strategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountMetricEvent {
    pub account_id: Uuid,
    pub entity_type: EntityType,
    pub timestamp: i64,
    pub balance: Decimal,
    pub equity: Decimal,
    pub min_equity: Decimal,
    pub max_equity: Decimal,
    pub drawdown: Decimal,
    pub drawdown_pct: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderEvent {
    pub order_id: Uuid,
    pub account_id: Uuid,
    pub rule_id: Uuid,
    pub symbol: String,
    pub side: OrderSide,
    pub size: Decimal,
    pub order_type: OrderType,
    pub trigger_price: Option<Decimal>,
    pub strategy: Option<Strategy>,
    pub timestamp: i64,
    pub status: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub enum ClientEvent {
    Trade(TradeExecutedEvent),
    Metric(AccountMetricEvent),
    Order(OrderEvent),
    Alert(serde_json::Value),
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct MqlTradeSignal {
    pub magic: u32,          // Magic integrity check (0x5449434B / 'TICK')
    pub event_type: u8,      // 0=Trade, 1=Order, 2=Alert, 3=Metric
    pub order_type: u8,      // 0=Market, 1=Limit, 2=Stop
    pub side: u8,            // 0=Buy, 1=Sell
    pub symbol: [u8; 32],    // Symbol name (null-padded, fixed size)
    pub signal_id: [u8; 16], // Unique UUID of the signal/alert
    pub size: f64,           // Double-precision order size
    pub price: f64,          // Double-precision execution price
    pub timestamp: i64,      // Unix timestamp in milliseconds (int64)
}

pub struct EventsClient {
    url: String,
    api_key: String,
    account_id: String,
}

impl EventsClient {
    pub fn new(url: &str, api_key: &str, account_id: &str) -> anyhow::Result<Self> {
        Ok(Self {
            url: url.to_string(),
            api_key: api_key.to_string(),
            account_id: account_id.to_string(),
        })
    }

    pub async fn stream(
        &self,
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<ClientEvent>>> {
        let mut url = Url::parse(&self.url)?;
        url.query_pairs_mut().append_pair("api_key", &self.api_key);
        url.query_pairs_mut()
            .append_pair("account_id", &self.account_id);

        let (ws_stream, _) = connect_async(url.as_str()).await?;
        let (_write, read) = ws_stream.split();

        let stream = read.filter_map(|msg| async move {
            match msg {
                Ok(Message::Binary(bytes)) => {
                    let client_event: Result<ClientEvent, _> = rmp_serde::from_slice(&bytes);
                    match client_event {
                        Ok(ce) => Some(Ok(ce)),
                        Err(err) => Some(Err(anyhow::anyhow!(
                            "Failed to parse binary MessagePack event data: {}",
                            err
                        ))),
                    }
                }
                Ok(Message::Text(_)) => Some(Err(anyhow::anyhow!(
                    "Received unexpected text frame in binary-only stream"
                ))),
                Ok(Message::Ping(_)) => None,
                Ok(Message::Pong(_)) => None,
                Ok(Message::Close(_)) => Some(Err(anyhow::anyhow!("WebSocket connection closed"))),
                Ok(_) => None,
                Err(e) => Some(Err(anyhow::anyhow!("WebSocket Error: {}", e))),
            }
        });

        Ok(stream)
    }
}

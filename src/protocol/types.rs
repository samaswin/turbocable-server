use serde::{Deserialize, Serialize};

/// Inbound commands from the WebSocket client.
///
/// Wire format (JSON, ActionCable-compatible):
/// ```json
/// {"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room_id\":1}"}
/// {"command":"message","identifier":"...","data":"{\"action\":\"speak\"}"}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ClientCommand {
    Subscribe { identifier: String },
    Unsubscribe { identifier: String },
    Message { identifier: String, data: String },
}

/// Outbound frames sent to the WebSocket client.
///
/// Wire format (JSON, ActionCable-compatible):
/// ```json
/// {"type":"welcome"}
/// {"type":"confirm_subscription","identifier":"..."}
/// {"type":"ping","message":1679012345}
/// {"type":"disconnect","reason":"unauthorized","reconnect":true}
/// {"type":"message","identifier":"...","message":{"text":"hello"}}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome,
    Ping {
        message: u64,
    },
    ConfirmSubscription {
        identifier: String,
    },
    RejectSubscription {
        identifier: String,
    },
    Disconnect {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reconnect: Option<bool>,
    },
    Message {
        identifier: String,
        message: serde_json::Value,
    },
}

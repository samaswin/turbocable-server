//! Protocol message types shared between JSON and MessagePack codecs.

use serde::{Deserialize, Serialize};

/// Inbound frame from the WebSocket client — either a capability hello or a command.
///
/// Deserialized with `#[serde(untagged)]`: a frame with `"type":"hello"` becomes
/// [`ClientFrame::Hello`]; a frame with a `"command"` field becomes
/// [`ClientFrame::Command`].  Using a single enum lets both frame kinds flow
/// through the same codec path (JSON or MessagePack) without a fragile
/// fallback-parse step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClientFrame {
    /// Capability negotiation and reconnect-replay handshake.
    Hello(HelloCommand),
    /// Standard ActionCable-compatible command (subscribe / unsubscribe / message).
    Command(ClientCommand),
}

/// Client-sent hello frame for connection setup and reconnect-replay negotiation.
///
/// Wire format (JSON):
/// ```json
/// {"type":"hello","last_seq":42,"capabilities":["replay_v1"]}
/// ```
/// Wire format (fresh connection, no replay):
/// ```json
/// {"type":"hello"}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloCommand {
    /// Discriminant — must equal `"hello"`.
    #[serde(rename = "type")]
    pub hello_type: HelloTag,
    /// Last JetStream stream sequence received before disconnecting.
    /// Absent on a fresh (non-reconnect) connection.
    #[serde(default)]
    pub last_seq: Option<u64>,
    /// Client capability flags.  Include `"replay_v1"` to signal support for
    /// sequence-based replay and at-least-once delivery semantics.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

impl HelloCommand {
    /// Returns `true` if the client advertised the `replay_v1` capability.
    pub fn is_replay_capable(&self) -> bool {
        self.capabilities.iter().any(|c| c == "replay_v1")
    }
}

/// Type discriminant that deserializes only from the string `"hello"`.
///
/// Prevents frames with a different `"type"` field (e.g. `"ping"`) from being
/// misidentified as a hello, ensuring the `#[serde(untagged)]` match on
/// [`ClientFrame`] falls through to [`ClientCommand`] correctly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HelloTag {
    #[serde(rename = "hello")]
    Hello,
}

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
        /// Present and `true` when the message was delivered via JetStream replay
        /// (reconnect catch-up), allowing clients to deduplicate.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replayed: Option<bool>,
        /// JetStream stream sequence number for client-side ordering and replay tracking.
        /// Clients send this back as `last_seq` on reconnect to resume from where they left off.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seq: Option<u64>,
    },
}

//! ActionCable-compatible JSON wire-format codec (`actioncable-v1-json`).

use bytes::Bytes;

use crate::errors::GatewayError;
use crate::protocol::types::{ClientFrame, ServerMessage};
use crate::protocol::Codec;

/// JSON codec implementing the `actioncable-v1-json` sub-protocol.
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn decode(&self, data: &[u8]) -> Result<ClientFrame, GatewayError> {
        serde_json::from_slice(data)
            .map_err(|e| GatewayError::Protocol(format!("invalid JSON frame: {e}")))
    }

    fn encode(&self, msg: &ServerMessage) -> Result<Bytes, GatewayError> {
        serde_json::to_vec(msg)
            .map(Bytes::from)
            .map_err(|e| GatewayError::Serialization(format!("JSON encode failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ClientCommand, HelloCommand, HelloTag};
    use serde_json::json;

    fn codec() -> JsonCodec {
        JsonCodec
    }

    // --- Decode ClientFrame::Command ---

    #[test]
    fn decode_subscribe() {
        let input = br#"{"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room_id\":1}"}"#;
        let frame = codec().decode(input).unwrap();
        assert_eq!(
            frame,
            ClientFrame::Command(ClientCommand::Subscribe {
                identifier: r#"{"channel":"ChatChannel","room_id":1}"#.into(),
            })
        );
    }

    #[test]
    fn decode_unsubscribe() {
        let input = br#"{"command":"unsubscribe","identifier":"test_channel"}"#;
        let frame = codec().decode(input).unwrap();
        assert_eq!(
            frame,
            ClientFrame::Command(ClientCommand::Unsubscribe {
                identifier: "test_channel".into(),
            })
        );
    }

    #[test]
    fn decode_message() {
        let input = br#"{"command":"message","identifier":"chat_1","data":"{\"action\":\"speak\",\"text\":\"hello\"}"}"#;
        let frame = codec().decode(input).unwrap();
        assert_eq!(
            frame,
            ClientFrame::Command(ClientCommand::Message {
                identifier: "chat_1".into(),
                data: r#"{"action":"speak","text":"hello"}"#.into(),
            })
        );
    }

    // --- Decode ClientFrame::Hello ---

    #[test]
    fn decode_hello_fresh_connection() {
        let input = br#"{"type":"hello"}"#;
        let frame = codec().decode(input).unwrap();
        assert_eq!(
            frame,
            ClientFrame::Hello(HelloCommand {
                hello_type: HelloTag::Hello,
                last_seq: None,
                capabilities: vec![],
            })
        );
    }

    #[test]
    fn decode_hello_with_last_seq() {
        let input = br#"{"type":"hello","last_seq":99}"#;
        let frame = codec().decode(input).unwrap();
        match frame {
            ClientFrame::Hello(h) => assert_eq!(h.last_seq, Some(99)),
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    #[test]
    fn decode_hello_replay_capable() {
        let input = br#"{"type":"hello","last_seq":7,"capabilities":["replay_v1"]}"#;
        let frame = codec().decode(input).unwrap();
        match frame {
            ClientFrame::Hello(h) => {
                assert_eq!(h.last_seq, Some(7));
                assert!(h.is_replay_capable());
            }
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    #[test]
    fn decode_hello_without_replay_capability() {
        let input = br#"{"type":"hello"}"#;
        let frame = codec().decode(input).unwrap();
        match frame {
            ClientFrame::Hello(h) => assert!(!h.is_replay_capable()),
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    // --- Encode ServerMessage ---

    #[test]
    fn encode_welcome() {
        let bytes = codec().encode(&ServerMessage::Welcome).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(val, json!({"type": "welcome"}));
    }

    #[test]
    fn encode_ping() {
        let msg = ServerMessage::Ping {
            message: 1_679_012_345,
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(val, json!({"type": "ping", "message": 1_679_012_345}));
    }

    #[test]
    fn encode_confirm_subscription() {
        let msg = ServerMessage::ConfirmSubscription {
            identifier: "chat_42".into(),
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({"type": "confirm_subscription", "identifier": "chat_42"})
        );
    }

    #[test]
    fn encode_reject_subscription() {
        let msg = ServerMessage::RejectSubscription {
            identifier: "secret_channel".into(),
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({"type": "reject_subscription", "identifier": "secret_channel"})
        );
    }

    #[test]
    fn encode_disconnect() {
        let msg = ServerMessage::Disconnect {
            reason: "unauthorized".into(),
            reconnect: Some(true),
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({"type": "disconnect", "reason": "unauthorized", "reconnect": true})
        );
    }

    #[test]
    fn encode_disconnect_without_reconnect() {
        let msg = ServerMessage::Disconnect {
            reason: "server_restart".into(),
            reconnect: None,
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({"type": "disconnect", "reason": "server_restart"})
        );
    }

    #[test]
    fn encode_broadcast_message() {
        let msg = ServerMessage::Message {
            identifier: "chat_1".into(),
            message: json!({"text": "hello", "sender": "alice"}),
            replayed: None,
            seq: None,
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({
                "type": "message",
                "identifier": "chat_1",
                "message": {"text": "hello", "sender": "alice"}
            })
        );
    }

    #[test]
    fn encode_replayed_message_includes_seq() {
        let msg = ServerMessage::Message {
            identifier: "chat_1".into(),
            message: json!({"text": "replayed msg"}),
            replayed: Some(true),
            seq: Some(8841),
        };
        let bytes = codec().encode(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            val,
            json!({
                "type": "message",
                "identifier": "chat_1",
                "message": {"text": "replayed msg"},
                "replayed": true,
                "seq": 8841
            })
        );
    }

    // --- Error handling ---

    #[test]
    fn decode_malformed_json_returns_error() {
        let input = b"not valid json {{{";
        let result = codec().decode(input);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, GatewayError::Protocol(_)));
    }

    #[test]
    fn decode_unknown_command_returns_error() {
        let input = br#"{"command":"unknown_cmd","identifier":"test"}"#;
        let result = codec().decode(input);
        assert!(result.is_err());
    }

    #[test]
    fn decode_missing_identifier_returns_error() {
        let input = br#"{"command":"subscribe"}"#;
        let result = codec().decode(input);
        assert!(result.is_err());
    }

    #[test]
    fn decode_missing_command_field_returns_error() {
        let input = br#"{"identifier":"test"}"#;
        let result = codec().decode(input);
        assert!(result.is_err());
    }

    #[test]
    fn decode_message_missing_data_returns_error() {
        let input = br#"{"command":"message","identifier":"chat_1"}"#;
        let result = codec().decode(input);
        assert!(result.is_err());
    }

    #[test]
    fn decode_empty_input_returns_error() {
        let result = codec().decode(b"");
        assert!(result.is_err());
    }

    #[test]
    fn decode_random_bytes_returns_error() {
        let result = codec().decode(&[0xff, 0xfe, 0x00, 0x01, 0xab]);
        assert!(result.is_err());
    }

    // --- Round-trip JSON ---

    #[test]
    fn json_round_trip_subscribe() {
        let original = ClientFrame::Command(ClientCommand::Subscribe {
            identifier: "room_99".into(),
        });
        let encoded = serde_json::to_vec(&original).unwrap();
        let decoded: ClientFrame = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn json_round_trip_hello() {
        let original = ClientFrame::Hello(HelloCommand {
            hello_type: HelloTag::Hello,
            last_seq: Some(123),
            capabilities: vec!["replay_v1".into()],
        });
        let encoded = serde_json::to_vec(&original).unwrap();
        let decoded: ClientFrame = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn json_round_trip_all_server_messages() {
        let messages = vec![
            ServerMessage::Welcome,
            ServerMessage::Ping { message: 42 },
            ServerMessage::ConfirmSubscription {
                identifier: "ch_1".into(),
            },
            ServerMessage::RejectSubscription {
                identifier: "ch_2".into(),
            },
            ServerMessage::Disconnect {
                reason: "idle".into(),
                reconnect: Some(false),
            },
            ServerMessage::Message {
                identifier: "ch_3".into(),
                message: json!({"key": "value"}),
                replayed: None,
                seq: None,
            },
        ];

        let c = codec();
        for msg in messages {
            let bytes = c.encode(&msg).unwrap();
            let decoded: ServerMessage = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(msg, decoded);
        }
    }
}

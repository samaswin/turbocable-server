use bytes::Bytes;

use crate::errors::GatewayError;
use crate::protocol::types::{ClientCommand, ServerMessage};
use crate::protocol::Codec;

pub struct MsgpackCodec;

impl Codec for MsgpackCodec {
    fn decode(&self, data: &[u8]) -> Result<ClientCommand, GatewayError> {
        rmp_serde::from_slice(data)
            .map_err(|e| GatewayError::Protocol(format!("invalid msgpack command: {e}")))
    }

    fn encode(&self, msg: &ServerMessage) -> Result<Bytes, GatewayError> {
        rmp_serde::to_vec_named(msg)
            .map(Bytes::from)
            .map_err(|e| GatewayError::Serialization(format!("msgpack encode failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn codec() -> MsgpackCodec {
        MsgpackCodec
    }

    // --- Round-trip ClientCommand ---

    #[test]
    fn round_trip_subscribe() {
        let original = ClientCommand::Subscribe {
            identifier: r#"{"channel":"ChatChannel","room_id":1}"#.into(),
        };
        let encoded = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: ClientCommand = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_unsubscribe() {
        let original = ClientCommand::Unsubscribe {
            identifier: "notifications".into(),
        };
        let encoded = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: ClientCommand = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_message() {
        let original = ClientCommand::Message {
            identifier: "chat_42".into(),
            data: r#"{"action":"speak","text":"hi"}"#.into(),
        };
        let encoded = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: ClientCommand = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    // --- Round-trip ServerMessage ---

    #[test]
    fn round_trip_welcome() {
        let c = codec();
        let original = ServerMessage::Welcome;
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_ping() {
        let c = codec();
        let original = ServerMessage::Ping {
            message: 1_700_000_000,
        };
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_confirm_subscription() {
        let c = codec();
        let original = ServerMessage::ConfirmSubscription {
            identifier: "chat_room_1".into(),
        };
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_reject_subscription() {
        let c = codec();
        let original = ServerMessage::RejectSubscription {
            identifier: "secret_room".into(),
        };
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_disconnect() {
        let c = codec();
        let original = ServerMessage::Disconnect {
            reason: "unauthorized".into(),
            reconnect: Some(true),
        };
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_broadcast_message() {
        let c = codec();
        let original = ServerMessage::Message {
            identifier: "chat_1".into(),
            message: json!({"text": "hello", "count": 42}),
        };
        let encoded = c.encode(&original).unwrap();
        let decoded = c.decode_server_msg(&encoded);
        assert_eq!(original, decoded);
    }

    // --- Round-trip via Codec trait ---

    #[test]
    fn codec_trait_round_trip_subscribe() {
        let c = codec();
        let original = ClientCommand::Subscribe {
            identifier: "room_5".into(),
        };
        let encoded = rmp_serde::to_vec_named(&original).unwrap();
        let decoded = c.decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn codec_trait_round_trip_message() {
        let c = codec();
        let original = ClientCommand::Message {
            identifier: "chat_99".into(),
            data: "payload".into(),
        };
        let encoded = rmp_serde::to_vec_named(&original).unwrap();
        let decoded = c.decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    // --- Error handling ---

    #[test]
    fn decode_empty_input_returns_error() {
        let result = codec().decode(b"");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GatewayError::Protocol(_)));
    }

    #[test]
    fn decode_random_bytes_returns_error() {
        let result = codec().decode(&[0xff, 0xfe, 0x00, 0x01, 0xab, 0xcd]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_valid_msgpack_but_wrong_shape_returns_error() {
        let wrong = rmp_serde::to_vec_named(&serde_json::json!({"foo": "bar"})).unwrap();
        let result = codec().decode(&wrong);
        assert!(result.is_err());
    }

    // --- Helpers ---

    impl MsgpackCodec {
        /// Test helper: decode a ServerMessage from msgpack bytes.
        fn decode_server_msg(&self, data: &[u8]) -> ServerMessage {
            rmp_serde::from_slice(data).expect("failed to decode ServerMessage from msgpack")
        }
    }
}

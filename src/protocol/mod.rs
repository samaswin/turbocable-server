//! WebSocket wire-format codecs: ActionCable-compatible JSON and binary MessagePack.

pub mod json;
pub mod msgpack;
pub mod types;

use bytes::Bytes;

use crate::errors::GatewayError;
use types::{ClientCommand, ServerMessage};

/// Wire-format codec selected during WebSocket sub-protocol negotiation.
///
/// Implementations: [`json::JsonCodec`] (actioncable-v1-json) and
/// [`msgpack::MsgpackCodec`] (turbocable-v1-msgpack).
pub trait Codec: Send + Sync {
    fn decode(&self, data: &[u8]) -> Result<ClientCommand, GatewayError>;
    fn encode(&self, msg: &ServerMessage) -> Result<Bytes, GatewayError>;
}

/// Sub-protocols advertised during the WebSocket handshake.
pub const SUB_PROTOCOL_JSON: &str = "actioncable-v1-json";
pub const SUB_PROTOCOL_MSGPACK: &str = "turbocable-v1-msgpack";

/// Return the appropriate codec for a negotiated sub-protocol string.
pub fn codec_for_protocol(protocol: &str) -> Option<Box<dyn Codec>> {
    match protocol {
        SUB_PROTOCOL_JSON => Some(Box::new(json::JsonCodec)),
        SUB_PROTOCOL_MSGPACK => Some(Box::new(msgpack::MsgpackCodec)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_for_json_protocol() {
        assert!(codec_for_protocol(SUB_PROTOCOL_JSON).is_some());
    }

    #[test]
    fn codec_for_msgpack_protocol() {
        assert!(codec_for_protocol(SUB_PROTOCOL_MSGPACK).is_some());
    }

    #[test]
    fn codec_for_unknown_protocol() {
        assert!(codec_for_protocol("unknown-v1").is_none());
    }

    #[test]
    fn json_codec_via_trait_object() {
        let codec = codec_for_protocol(SUB_PROTOCOL_JSON).unwrap();
        let input = br#"{"command":"subscribe","identifier":"ch_1"}"#;
        let cmd = codec.decode(input).unwrap();
        assert_eq!(
            cmd,
            types::ClientCommand::Subscribe {
                identifier: "ch_1".into()
            }
        );

        let msg = types::ServerMessage::Welcome;
        let encoded = codec.encode(&msg).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn msgpack_codec_via_trait_object() {
        let codec = codec_for_protocol(SUB_PROTOCOL_MSGPACK).unwrap();
        let original = types::ClientCommand::Subscribe {
            identifier: "ch_2".into(),
        };
        let packed = rmp_serde::to_vec_named(&original).unwrap();
        let decoded = codec.decode(&packed).unwrap();
        assert_eq!(original, decoded);

        let msg = types::ServerMessage::Ping { message: 99 };
        let encoded = codec.encode(&msg).unwrap();
        assert!(!encoded.is_empty());
    }
}

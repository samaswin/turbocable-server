//! Fuzz target: MessagePack → ClientFrame parser.
//!
//! Verifies that the msgpack codec never panics on arbitrary byte input.
//! Errors are acceptable; panics and unwrap failures are not.
//!
//! Run with:
//!   cargo fuzz run msgpack_decode -- -max_total_time=60
#![no_main]

use libfuzzer_sys::fuzz_target;
use turbocable_server::protocol::{msgpack::MsgpackCodec, Codec};

fuzz_target!(|data: &[u8]| {
    let _ = MsgpackCodec.decode(data);
});

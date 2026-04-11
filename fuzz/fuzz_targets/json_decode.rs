//! Fuzz target: JSON → ClientFrame parser.
//!
//! Verifies that the JSON codec never panics, no matter what bytes it receives.
//! Errors are acceptable; panics and unwrap failures are not.
//!
//! Run with:
//!   cargo fuzz run json_decode -- -max_total_time=60
#![no_main]

use libfuzzer_sys::fuzz_target;
use turbocable_server::protocol::{json::JsonCodec, Codec};

fuzz_target!(|data: &[u8]| {
    let _ = JsonCodec.decode(data);
});

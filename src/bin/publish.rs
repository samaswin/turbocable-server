//! `tc-publish` — load-test message publisher for TurboCable.
//!
//! Publishes messages with monotonic sequence numbers and millisecond timestamps to
//! a NATS JetStream subject, enabling k6 clients to measure fan-out latency and
//! detect zero message loss.
//!
//! # Usage
//!
//! ```bash
//! # Publish 10 msg/s for 10 minutes to the "bench" stream
//! ./tc-publish --stream bench --rate 10 --duration 600
//!
//! # High-throughput burst (100 msg/s for 5 minutes)
//! ./tc-publish --stream bench --rate 100 --duration 300
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;

/// Publish load-test messages to a NATS JetStream subject.
#[derive(Parser, Debug)]
#[command(name = "tc-publish", about = "TurboCable load-test message publisher")]
struct Args {
    /// NATS server URL.
    #[arg(
        long,
        env = "TURBOCABLE_NATS_URL",
        default_value = "nats://localhost:4222"
    )]
    nats_url: String,

    /// Stream name to publish to (subject: `TURBOCABLE.<stream>`).
    #[arg(long, default_value = "bench")]
    stream: String,

    /// Target publish rate in messages per second.
    #[arg(long, default_value = "10")]
    rate: u64,

    /// How long to publish in seconds.
    #[arg(long, default_value = "600")]
    duration: u64,

    /// Suppress periodic progress output.
    #[arg(long, default_value = "false")]
    quiet: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let client = async_nats::connect(&args.nats_url).await?;
    let subject = format!("TURBOCABLE.{}", args.stream);

    let interval = Duration::from_nanos(1_000_000_000 / args.rate.max(1));
    let end = tokio::time::Instant::now() + Duration::from_secs(args.duration);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut seq: u64 = 0;

    eprintln!(
        "Publishing to {} at {} msg/s for {}s (subject: {})",
        args.nats_url, args.rate, args.duration, subject
    );

    loop {
        ticker.tick().await;
        if tokio::time::Instant::now() >= end {
            break;
        }

        let sent_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let payload = format!(
            r#"{{"seq":{seq},"sent_at":{sent_at},"stream":"{}"}}"#,
            args.stream
        );

        client
            .publish(subject.clone(), payload.into())
            .await
            .map_err(|e| format!("publish error at seq={seq}: {e}"))?;

        seq += 1;

        if !args.quiet && seq.is_multiple_of(100) {
            eprintln!("Published {seq} messages");
        }
    }

    client.flush().await?;
    println!("Done. Published {seq} messages to {subject}");
    Ok(())
}

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "turbocable-server", version, about)]
pub struct Config {
    #[arg(long, env = "TURBOCABLE_PORT", default_value = "9292")]
    pub port: u16,

    #[arg(
        long,
        env = "TURBOCABLE_NATS_URL",
        default_value = "nats://localhost:4222"
    )]
    pub nats_url: String,

    #[arg(long, env = "TURBOCABLE_NODE_ID", default_value_t = default_node_id())]
    pub node_id: String,

    #[arg(long, env = "TURBOCABLE_PING_INTERVAL", default_value = "30")]
    pub ping_interval_secs: u64,
}

fn default_node_id() -> String {
    format!("node_{}", uuid::Uuid::new_v4().simple())
}

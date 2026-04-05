use std::time::Duration;

/// How the client discovers Ruxio servers.
pub enum MembershipConfig {
    /// Fixed list of server addresses (`host:port`).
    Static { servers: Vec<String> },
    /// etcd-based discovery with watch for membership changes.
    Etcd {
        endpoints: Vec<String>,
        prefix: String,
    },
}

/// Client configuration.
pub struct ClientConfig {
    /// How to discover servers.
    pub membership: MembershipConfig,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Read timeout per response.
    pub read_timeout: Duration,
    /// Max retries for retriable errors (including redirects).
    pub max_retries: u32,
    /// Virtual nodes per physical node in the hash ring.
    /// Must match the server's `vnodes_per_node` setting.
    pub vnodes_per_node: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            membership: MembershipConfig::Static {
                servers: vec!["127.0.0.1:51234".to_string()],
            },
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
            max_retries: 3,
            vnodes_per_node: 150,
        }
    }
}

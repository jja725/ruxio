use std::time::Duration;

/// How the client routes requests to servers.
pub enum RoutingStrategy {
    /// Client builds a consistent hash ring and sends each request
    /// directly to the owning server by file path. Fastest — no redirects
    /// on the happy path. Requires membership discovery (static or etcd).
    ClientSideHashRing,
    /// Client sends all requests to any available server. The server
    /// routes internally (via Redirect responses). Simpler client setup
    /// but adds a network hop on misrouted requests.
    ServerSideRouting,
}

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
    /// How to route requests to servers.
    pub routing: RoutingStrategy,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Read timeout per response.
    pub read_timeout: Duration,
    /// Max retries for retriable errors (including redirects).
    pub max_retries: u32,
    /// Virtual nodes per physical node in the hash ring.
    /// Must match the server's `vnodes_per_node` setting.
    /// Only used with `RoutingStrategy::ClientSideHashRing`.
    pub vnodes_per_node: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            membership: MembershipConfig::Static {
                servers: vec!["127.0.0.1:51234".to_string()],
            },
            routing: RoutingStrategy::ClientSideHashRing,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
            max_retries: 3,
            vnodes_per_node: 150,
        }
    }
}

use crate::ring::{HashRing, NodeId};

/// Discovery mode for finding cluster peers.
#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Static list of peer addresses.
    Static { peers: Vec<String> },
    /// etcd-based service discovery.
    Etcd {
        endpoints: Vec<String>,
        prefix: String,
    },
}

/// Manages cluster membership and the consistent hash ring.
///
/// Each node maintains its own view of the ring. Membership changes
/// (joins/leaves) are propagated via the control plane (HTTP/2).
pub struct ClusterMembership {
    ring: HashRing,
    self_id: NodeId,
    mode: DiscoveryMode,
}

impl ClusterMembership {
    pub fn new(self_id: NodeId, vnodes: usize, mode: DiscoveryMode) -> Self {
        let mut ring = HashRing::new(vnodes);
        ring.add_node(self_id.clone());
        Self {
            ring,
            self_id,
            mode,
        }
    }

    /// Check if a file path is owned by this node.
    pub fn is_local(&self, file_path: &str) -> bool {
        self.ring
            .get_node(file_path)
            .map(|n| n == &self.self_id)
            .unwrap_or(false)
    }

    /// Get the owning node for a file path.
    pub fn owner(&self, file_path: &str) -> Option<&NodeId> {
        self.ring.get_node(file_path)
    }

    /// Add a peer to the cluster.
    pub fn add_peer(&mut self, node: NodeId) {
        self.ring.add_node(node);
    }

    /// Remove a peer from the cluster.
    pub fn remove_peer(&mut self, node: &NodeId) {
        self.ring.remove_node(node);
    }

    /// This node's ID.
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// Reference to the underlying ring.
    pub fn ring(&self) -> &HashRing {
        &self.ring
    }

    /// Discovery mode.
    pub fn discovery_mode(&self) -> &DiscoveryMode {
        &self.mode
    }

    /// Initialize from static peer list or etcd.
    pub async fn discover_peers(&mut self) -> anyhow::Result<()> {
        match &self.mode {
            DiscoveryMode::Static { peers } => {
                for peer in peers {
                    if peer != &self.self_id.0 {
                        self.ring.add_node(NodeId(peer.clone()));
                    }
                }
            }
            DiscoveryMode::Etcd { .. } => {
                // TODO: implement etcd-based discovery
                tracing::warn!("etcd discovery not yet implemented, running in standalone mode");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_local() {
        let self_id = NodeId::new("localhost", 8080);
        let membership = ClusterMembership::new(
            self_id.clone(),
            100,
            DiscoveryMode::Static { peers: vec![] },
        );

        // With single node, everything is local
        assert!(membership.is_local("gs://bucket/file.parquet"));
    }

    #[test]
    fn test_membership_with_peers() {
        let self_id = NodeId::new("node1", 8080);
        let mut membership = ClusterMembership::new(
            self_id.clone(),
            100,
            DiscoveryMode::Static { peers: vec![] },
        );
        membership.add_peer(NodeId::new("node2", 8080));
        membership.add_peer(NodeId::new("node3", 8080));

        // Not all files should be local anymore
        let mut local_count = 0;
        for i in 0..300 {
            if membership.is_local(&format!("gs://bucket/file-{i}.parquet")) {
                local_count += 1;
            }
        }
        // Should own roughly 1/3 of keys
        assert!(local_count > 50, "too few local keys: {local_count}");
        assert!(local_count < 200, "too many local keys: {local_count}");
    }
}

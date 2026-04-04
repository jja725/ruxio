use std::cell::RefCell;
use std::sync::mpsc;

use crate::ring::{HashRing, NodeId};
use crate::service::{MembershipEvent, MembershipService};

/// Manages cluster membership and the consistent hash ring.
///
/// Wraps a `MembershipService` implementation (static, etcd, or future raft)
/// and maintains a local hash ring that is updated when membership changes.
///
/// Each monoio worker thread should have its own `ClusterMembership` instance
/// sharing the same underlying `MembershipService` (via `Arc`).
pub struct ClusterMembership {
    service: Box<dyn MembershipService>,
    ring: RefCell<HashRing>,
    _vnodes: usize,
    event_rx: mpsc::Receiver<MembershipEvent>,
}

impl ClusterMembership {
    /// Create a new membership manager wrapping the given service.
    pub fn new(service: Box<dyn MembershipService>, vnodes: usize) -> Self {
        let event_rx = service.subscribe();
        let mut ring = HashRing::new(vnodes);
        ring.add_node(service.self_id().clone());
        Self {
            service,
            ring: RefCell::new(ring),
            _vnodes: vnodes,
            event_rx,
        }
    }

    /// Join the cluster and build the initial hash ring from live members.
    pub fn start(&self) -> anyhow::Result<()> {
        self.service
            .join()
            .map_err(|e| anyhow::anyhow!("membership join failed: {e}"))?;

        let members = self
            .service
            .get_live_members()
            .map_err(|e| anyhow::anyhow!("get members failed: {e}"))?;

        self.ring.borrow_mut().rebuild(&members);
        ruxio_common::metrics::CLUSTER_MEMBERS.set(members.len() as i64);
        tracing::info!("Cluster membership started: {} members", members.len());
        Ok(())
    }

    /// Check if a file path is owned by this node.
    pub fn is_local(&self, file_path: &str) -> bool {
        self.ring
            .borrow()
            .get_node(file_path)
            .map(|n| n == self.service.self_id())
            .unwrap_or(false)
    }

    /// Get the owning node for a file path.
    /// Returns an owned `NodeId` because we borrow through `RefCell`.
    pub fn owner(&self, file_path: &str) -> Option<NodeId> {
        self.ring.borrow().get_node(file_path).cloned()
    }

    /// This node's ID.
    pub fn self_id(&self) -> &NodeId {
        self.service.self_id()
    }

    /// Reference to the underlying ring (borrowed).
    pub fn ring(&self) -> std::cell::Ref<'_, HashRing> {
        self.ring.borrow()
    }

    /// Leave the cluster and deregister.
    pub fn leave(&self) {
        if let Err(e) = self.service.leave() {
            tracing::warn!("Failed to leave cluster: {e}");
        }
    }

    /// Poll for membership change events and update the ring.
    /// Should be called periodically from each worker thread.
    pub fn poll_events(&self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                MembershipEvent::Joined(node) => {
                    tracing::info!("Node joined: {node}");
                    self.ring.borrow_mut().add_node(node);
                    ruxio_common::metrics::CLUSTER_MEMBERS
                        .set(self.ring.borrow().node_count() as i64);
                }
                MembershipEvent::Left(node) => {
                    tracing::info!("Node left: {node}");
                    self.ring.borrow_mut().remove_node(&node);
                    ruxio_common::metrics::CLUSTER_MEMBERS
                        .set(self.ring.borrow().node_count() as i64);
                }
                MembershipEvent::Snapshot(members) => {
                    tracing::info!("Membership snapshot: {} members", members.len());
                    self.ring.borrow_mut().rebuild(&members);
                    ruxio_common::metrics::CLUSTER_MEMBERS.set(members.len() as i64);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::static_membership::StaticMembership;

    #[test]
    fn test_membership_local() {
        let self_id = NodeId::new("localhost", 8080);
        let svc = StaticMembership::new(self_id.clone(), vec![]);
        let membership = ClusterMembership::new(Box::new(svc), 100);

        // With single node, everything is local
        assert!(membership.is_local("gs://bucket/file.parquet"));
    }

    #[test]
    fn test_membership_with_peers() {
        let self_id = NodeId::new("node1", 8080);
        let peers = vec![NodeId::new("node2", 8080), NodeId::new("node3", 8080)];
        let svc = StaticMembership::new(self_id.clone(), peers);
        let membership = ClusterMembership::new(Box::new(svc), 100);
        membership.start().unwrap();

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

    #[test]
    fn test_owner_returns_owned() {
        let self_id = NodeId::new("localhost", 8080);
        let svc = StaticMembership::new(self_id.clone(), vec![]);
        let membership = ClusterMembership::new(Box::new(svc), 100);

        let owner = membership.owner("gs://bucket/file.parquet");
        assert_eq!(owner, Some(NodeId::new("localhost", 8080)));
    }

    #[test]
    fn test_leave_noop_static() {
        let self_id = NodeId::new("localhost", 8080);
        let svc = StaticMembership::new(self_id.clone(), vec![]);
        let membership = ClusterMembership::new(Box::new(svc), 100);
        membership.leave(); // Should not panic
    }
}

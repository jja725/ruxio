use std::sync::mpsc;

use crate::ring::NodeId;
use crate::service::{MembershipError, MembershipEvent, MembershipService};

/// Static membership: peers are known at startup and never change.
///
/// This is the simplest membership backend, suitable for development
/// and small fixed-size clusters.
pub struct StaticMembership {
    self_id: NodeId,
    peers: Vec<NodeId>,
}

impl StaticMembership {
    pub fn new(self_id: NodeId, peers: Vec<NodeId>) -> Self {
        Self { self_id, peers }
    }
}

impl MembershipService for StaticMembership {
    fn join(&self) -> Result<(), MembershipError> {
        Ok(()) // No-op for static membership
    }

    fn leave(&self) -> Result<(), MembershipError> {
        Ok(()) // No-op for static membership
    }

    fn get_live_members(&self) -> Result<Vec<NodeId>, MembershipError> {
        let mut members = vec![self.self_id.clone()];
        members.extend(self.peers.iter().cloned());
        Ok(members)
    }

    fn subscribe(&self) -> mpsc::Receiver<MembershipEvent> {
        // Static membership never changes, so return a closed channel.
        let (_tx, rx) = mpsc::channel();
        rx
    }

    fn self_id(&self) -> &NodeId {
        &self.self_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_membership_single_node() {
        let svc = StaticMembership::new(NodeId::new("localhost", 8080), vec![]);
        assert_eq!(svc.self_id(), &NodeId::new("localhost", 8080));
        let members = svc.get_live_members().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0], NodeId::new("localhost", 8080));
    }

    #[test]
    fn test_static_membership_with_peers() {
        let peers = vec![NodeId::new("node2", 8080), NodeId::new("node3", 8080)];
        let svc = StaticMembership::new(NodeId::new("node1", 8080), peers);
        let members = svc.get_live_members().unwrap();
        assert_eq!(members.len(), 3);
    }

    #[test]
    fn test_static_join_leave_noop() {
        let svc = StaticMembership::new(NodeId::new("localhost", 8080), vec![]);
        assert!(svc.join().is_ok());
        assert!(svc.leave().is_ok());
    }

    #[test]
    fn test_static_subscribe_closed() {
        let svc = StaticMembership::new(NodeId::new("localhost", 8080), vec![]);
        let rx = svc.subscribe();
        // Channel should be immediately closed (sender dropped)
        assert!(rx.try_recv().is_err());
    }
}

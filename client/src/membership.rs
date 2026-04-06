use std::cell::{Cell, RefCell};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ruxio_cluster::ring::{HashRing, NodeId};
use ruxio_cluster::service::MembershipEvent;

/// Client-side membership view with consistent hash ring.
///
/// Maintains a local hash ring and refreshes it by polling membership events
/// on a configurable interval. Also supports on-demand refresh when a
/// Redirect response indicates the ring is stale.
pub(crate) struct ClientMembership {
    ring: RefCell<HashRing>,
    event_rx: mpsc::Receiver<MembershipEvent>,
    refresh_interval: Duration,
    last_refresh: Cell<Instant>,
}

impl ClientMembership {
    /// Create with an initial member list and event receiver.
    pub fn new(
        initial_members: Vec<NodeId>,
        vnodes_per_node: usize,
        event_rx: mpsc::Receiver<MembershipEvent>,
        refresh_interval: Duration,
    ) -> Self {
        let mut ring = HashRing::new(vnodes_per_node);
        ring.rebuild(&initial_members);
        Self {
            ring: RefCell::new(ring),
            event_rx,
            refresh_interval,
            last_refresh: Cell::new(Instant::now()),
        }
    }

    /// Find the owning node for a file URI via consistent hashing.
    ///
    /// Automatically refreshes the ring if the refresh interval has elapsed.
    pub fn owner(&self, uri: &str) -> Option<NodeId> {
        self.maybe_refresh();
        self.ring.borrow().get_node(uri).cloned()
    }

    /// Get multiple candidate nodes for a URI, ordered by ring position.
    pub fn candidates(&self, uri: &str, n: usize) -> Vec<NodeId> {
        self.maybe_refresh();
        self.ring
            .borrow()
            .get_nodes(uri, n)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get all current nodes.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.ring.borrow().nodes().to_vec()
    }

    /// Force an immediate refresh by draining all pending membership events.
    ///
    /// Called when the client receives a Redirect, which signals a stale ring.
    pub fn force_refresh(&self) -> bool {
        let changed = self.drain_events();
        self.last_refresh.set(Instant::now());
        changed
    }

    /// Poll events if the refresh interval has elapsed.
    fn maybe_refresh(&self) {
        if self.last_refresh.get().elapsed() >= self.refresh_interval {
            self.drain_events();
            self.last_refresh.set(Instant::now());
        }
    }

    /// Drain all pending membership events and update the ring.
    /// Returns `true` if the ring was modified.
    fn drain_events(&self) -> bool {
        let mut changed = false;
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                MembershipEvent::Joined(node) => {
                    tracing::info!("Node joined: {node}");
                    self.ring.borrow_mut().add_node(node);
                    changed = true;
                }
                MembershipEvent::Left(node) => {
                    tracing::info!("Node left: {node}");
                    self.ring.borrow_mut().remove_node(&node);
                    changed = true;
                }
                MembershipEvent::Snapshot(members) => {
                    tracing::info!("Membership snapshot: {} members", members.len());
                    self.ring.borrow_mut().rebuild(&members);
                    changed = true;
                }
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_nodes(n: usize) -> Vec<NodeId> {
        (0..n)
            .map(|i| NodeId::new(format!("node{i}"), 8080))
            .collect()
    }

    #[test]
    fn test_static_membership_routes() {
        let nodes = make_nodes(3);
        let (_tx, rx) = mpsc::channel();
        let membership = ClientMembership::new(nodes, 150, rx, Duration::from_secs(5));

        for i in 0..100 {
            let uri = format!("gs://bucket/file{i}.parquet");
            assert!(membership.owner(&uri).is_some());
        }
    }

    #[test]
    fn test_poll_join_event() {
        let nodes = make_nodes(2);
        let (tx, rx) = mpsc::channel();
        let membership = ClientMembership::new(nodes, 150, rx, Duration::from_secs(5));
        assert_eq!(membership.nodes().len(), 2);

        tx.send(MembershipEvent::Joined(NodeId::new("node2", 8080)))
            .unwrap();
        assert!(membership.force_refresh());
        assert_eq!(membership.nodes().len(), 3);
    }

    #[test]
    fn test_poll_leave_event() {
        let nodes = make_nodes(3);
        let (tx, rx) = mpsc::channel();
        let membership = ClientMembership::new(nodes, 150, rx, Duration::from_secs(5));

        tx.send(MembershipEvent::Left(NodeId::new("node1", 8080)))
            .unwrap();
        assert!(membership.force_refresh());
        assert_eq!(membership.nodes().len(), 2);
    }

    #[test]
    fn test_poll_snapshot_replaces_ring() {
        let nodes = make_nodes(3);
        let (tx, rx) = mpsc::channel();
        let membership = ClientMembership::new(nodes, 150, rx, Duration::from_secs(5));

        let new_nodes = make_nodes(5);
        tx.send(MembershipEvent::Snapshot(new_nodes)).unwrap();
        assert!(membership.force_refresh());
        assert_eq!(membership.nodes().len(), 5);
    }

    #[test]
    fn test_no_events_returns_false() {
        let nodes = make_nodes(2);
        let (_tx, rx) = mpsc::channel();
        let membership = ClientMembership::new(nodes, 150, rx, Duration::from_secs(5));
        assert!(!membership.force_refresh());
    }
}

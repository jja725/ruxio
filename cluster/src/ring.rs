use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher};

use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self(format!("{}:{}", host.into(), port))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Consistent hash ring with virtual nodes.
///
/// Maps file paths to nodes using consistent hashing. Virtual nodes ensure
/// even distribution of keys across the ring. When nodes join/leave, only
/// ~1/N of keys are remapped (where N is the number of nodes).
pub struct HashRing {
    ring: BTreeMap<u64, NodeId>,
    vnodes_per_node: usize,
    nodes: Vec<NodeId>,
}

impl HashRing {
    /// Create a new hash ring.
    ///
    /// `vnodes_per_node` controls distribution uniformity. Higher values
    /// give better balance but use more memory. 150-200 is typical.
    pub fn new(vnodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node,
            nodes: Vec::new(),
        }
    }

    /// Add a node to the ring.
    pub fn add_node(&mut self, node: NodeId) {
        for i in 0..self.vnodes_per_node {
            let hash = hash_vnode(&node, i);
            self.ring.insert(hash, node.clone());
        }
        self.nodes.push(node);
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node: &NodeId) {
        for i in 0..self.vnodes_per_node {
            let hash = hash_vnode(node, i);
            self.ring.remove(&hash);
        }
        self.nodes.retain(|n| n != node);
    }

    /// Look up which node owns a given key (file path).
    ///
    /// Finds the first node on the ring whose position is >= the key's hash.
    /// If no such node exists (key hashes past all nodes), wraps around to
    /// the first node on the ring.
    pub fn get_node(&self, key: &str) -> Option<&NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = hash_key(key);

        // Find first node >= hash (clockwise search)
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node)
    }

    /// Get N nodes for a key (for replication or fallback).
    pub fn get_nodes(&self, key: &str, n: usize) -> Vec<&NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }
        let hash = hash_key(key);
        let mut result = Vec::with_capacity(n);
        let mut seen = std::collections::HashSet::new();

        // Walk clockwise from the hash position
        for (_, node) in self.ring.range(hash..).chain(self.ring.iter()) {
            if seen.insert(node) {
                result.push(node);
                if result.len() >= n {
                    break;
                }
            }
        }
        result
    }

    /// Number of physical nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// All nodes in the ring.
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Rebuild the ring from a complete member list.
    /// Clears all existing nodes and re-adds the given members.
    pub fn rebuild(&mut self, members: &[NodeId]) {
        self.ring.clear();
        self.nodes.clear();
        for member in members {
            self.add_node(member.clone());
        }
    }
}

fn hash_key(key: &str) -> u64 {
    Xxh3DefaultBuilder.hash_one(key)
}

fn hash_vnode(node: &NodeId, vnode_idx: usize) -> u64 {
    let mut hasher = Xxh3DefaultBuilder.build_hasher();
    node.0.hash(&mut hasher);
    vnode_idx.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node() {
        let mut ring = HashRing::new(100);
        let node = NodeId::new("localhost", 8080);
        ring.add_node(node.clone());

        // All keys should map to the single node
        assert_eq!(ring.get_node("file1.parquet"), Some(&node));
        assert_eq!(ring.get_node("file2.parquet"), Some(&node));
    }

    #[test]
    fn test_distribution() {
        let mut ring = HashRing::new(150);
        let n1 = NodeId::new("node1", 8080);
        let n2 = NodeId::new("node2", 8080);
        let n3 = NodeId::new("node3", 8080);
        ring.add_node(n1.clone());
        ring.add_node(n2.clone());
        ring.add_node(n3.clone());

        // Generate many keys and check distribution
        let mut counts = std::collections::HashMap::new();
        for i in 0..3000 {
            let key = format!("gs://bucket/table/part-{i:05}.parquet");
            let node = ring.get_node(&key).unwrap();
            *counts.entry(node.clone()).or_insert(0u32) += 1;
        }

        // Each node should get roughly 1000 keys (within 50% tolerance)
        for count in counts.values() {
            assert!(*count > 500, "node got too few keys: {count}");
            assert!(*count < 1500, "node got too many keys: {count}");
        }
    }

    #[test]
    fn test_node_removal_minimal_disruption() {
        let mut ring = HashRing::new(150);
        let n1 = NodeId::new("node1", 8080);
        let n2 = NodeId::new("node2", 8080);
        let n3 = NodeId::new("node3", 8080);
        ring.add_node(n1.clone());
        ring.add_node(n2.clone());
        ring.add_node(n3.clone());

        // Record assignments before removal
        let keys: Vec<String> = (0..1000)
            .map(|i| format!("gs://bucket/file-{i}.parquet"))
            .collect();
        let before: Vec<NodeId> = keys
            .iter()
            .map(|k| ring.get_node(k).unwrap().clone())
            .collect();

        // Remove node2
        ring.remove_node(&n2);

        let after: Vec<NodeId> = keys
            .iter()
            .map(|k| ring.get_node(k).unwrap().clone())
            .collect();

        // Count how many keys changed assignment
        let changed = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b != a)
            .count();

        // Should be roughly 1/3 of keys (the ones that were on node2)
        // Allow generous tolerance
        assert!(
            changed < 600,
            "too many keys changed: {changed}/1000 (expected ~333)"
        );
    }

    #[test]
    fn test_get_nodes_replication() {
        let mut ring = HashRing::new(100);
        ring.add_node(NodeId::new("n1", 8080));
        ring.add_node(NodeId::new("n2", 8080));
        ring.add_node(NodeId::new("n3", 8080));

        let nodes = ring.get_nodes("some-file.parquet", 2);
        assert_eq!(nodes.len(), 2);
        assert_ne!(nodes[0], nodes[1]);
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::new(100);
        assert_eq!(ring.get_node("any-key"), None);
        assert_eq!(ring.node_count(), 0);
    }
}

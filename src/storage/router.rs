//! Ring-based replica set selection for the storage layer.

use super::table::Key;
use super::StorageError;
use crate::hash_ring::{HashRing, RingNode};

/// The set of nodes that own replicas for a given key.
#[derive(Debug, Clone)]
pub struct ReplicaSet {
    /// All replica nodes in ring order. Length == `rf` (or fewer if the ring is small).
    pub nodes: Vec<RingNode>,
    /// Primary owner (first replica in ring order for the key).
    pub primary: RingNode,
}

/// Routes keys to their replica sets using a consistent-hash ring.
#[derive(Clone)]
pub struct StorageRouter {
    ring: HashRing,
    local_node_id: String,
    rf: usize,
}

impl StorageRouter {
    pub fn new(ring: HashRing, local_node_id: String, rf: usize) -> Self {
        Self {
            ring,
            local_node_id,
            rf,
        }
    }

    /// Return the `rf`-node replica set for `key`.
    pub fn replica_set_for(&self, key: &Key) -> Result<ReplicaSet, StorageError> {
        let nodes: Vec<RingNode> = self.ring.get_nodes(key, self.rf)
            .into_iter()
            .cloned()
            .collect();

        if nodes.len() < self.rf && nodes.len() < self.ring.node_count() {
            return Err(StorageError(format!(
                "not enough ring nodes: need {}, have {}",
                self.rf,
                self.ring.node_count()
            )));
        }
        if nodes.is_empty() {
            return Err(StorageError("hash ring is empty".into()));
        }

        let primary = nodes[0].clone();
        Ok(ReplicaSet { nodes, primary })
    }

    /// `true` if this node is the primary owner of `key`.
    pub fn is_local_primary(&self, key: &Key) -> bool {
        self.ring
            .get_node(key)
            .map(|n| n.id == self.local_node_id)
            .unwrap_or(false)
    }

    /// `true` if this node appears in the replica set for `key`.
    pub fn is_local_replica(&self, key: &Key) -> bool {
        self.ring
            .get_nodes(key, self.rf)
            .iter()
            .any(|n| n.id == self.local_node_id)
    }

    /// Nodes in `replica_set` that belong to `dc` (filtered by `RingNode::dc`).
    /// Nodes whose `dc` field is `None` are excluded.
    pub fn local_replicas<'a>(&self, replicas: &'a ReplicaSet, dc: &str) -> Vec<&'a RingNode> {
        replicas
            .nodes
            .iter()
            .filter(|n| n.dc.as_deref() == Some(dc))
            .collect()
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn rf(&self) -> usize {
        self.rf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash_ring::RingNode;

    fn three_node_ring() -> HashRing {
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::new("n1", "127.0.0.1", 9001));
        ring.add_node(RingNode::new("n2", "127.0.0.1", 9002));
        ring.add_node(RingNode::new("n3", "127.0.0.1", 9003));
        ring
    }

    fn key(s: &str) -> Key {
        Key::from(s.as_bytes().to_vec())
    }

    #[test]
    fn three_nodes_rf3_returns_three_replicas() {
        let ring = three_node_ring();
        let router = StorageRouter::new(ring, "n1".into(), 3);
        let rs = router.replica_set_for(&key("some-key")).unwrap();
        assert_eq!(rs.nodes.len(), 3);
    }

    #[test]
    fn is_local_primary_correct() {
        let ring = three_node_ring();
        // find the primary for a known key then assert is_local_primary matches
        let primary_id = ring.get_node(&key("some-key")).unwrap().id.clone();
        let router = StorageRouter::new(ring, primary_id.clone(), 3);
        assert!(router.is_local_primary(&key("some-key")));
        // a non-primary node
        let other_id = ["n1", "n2", "n3"]
            .iter()
            .find(|&&id| id != primary_id.as_str())
            .unwrap()
            .to_string();
        let ring2 = three_node_ring();
        let router2 = StorageRouter::new(ring2, other_id, 3);
        assert!(!router2.is_local_primary(&key("some-key")));
    }

    #[test]
    fn two_node_ring_rf3_returns_available_nodes() {
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::new("n1", "127.0.0.1", 9001));
        ring.add_node(RingNode::new("n2", "127.0.0.1", 9002));
        let router = StorageRouter::new(ring, "n1".into(), 3);
        // Should not error — returns 2 nodes (all available), rf=3 cannot be satisfied
        // but we don't error when nodes < rf if we return all available nodes
        let rs = router.replica_set_for(&key("k")).unwrap();
        assert_eq!(rs.nodes.len(), 2); // all available
    }

    #[test]
    fn empty_ring_returns_error() {
        let ring = HashRing::new(150);
        let router = StorageRouter::new(ring, "n1".into(), 3);
        assert!(router.replica_set_for(&key("k")).is_err());
    }
}

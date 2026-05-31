//! Consistent-hash ring for cluster node discovery.

use std::collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Physical cluster node (id + host + port). Distinct from TCP [`crate::distributed::Node`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingNode {
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl RingNode {
    pub fn new(id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            id: id.into(),
            host: host.into(),
            port,
        }
    }

    /// Parse `"host:port"` (e.g. `"127.0.0.1:65140"`).
    pub fn from_socket_addr(id: impl Into<String>, addr: &str) -> Self {
        let id = id.into();
        let (host, port) = parse_host_port(addr);
        Self { id, host, port }
    }

    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Hash for RingNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Consistent-hash ring with virtual nodes for even key distribution.
#[derive(Debug, Clone)]
pub struct HashRing {
    nodes: HashMap<String, RingNode>,
    ring: BTreeMap<u64, String>,
    virtual_nodes: u32,
}

impl HashRing {
    pub fn new(virtual_nodes: u32) -> Self {
        Self {
            nodes: HashMap::new(),
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    fn hash_key<T: Hash>(&self, key: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn add_node(&mut self, node: RingNode) {
        for i in 0..self.virtual_nodes {
            let virtual_key = format!("{}:vnode:{}", node.id, i);
            let hash = self.hash_key(&virtual_key);
            self.ring.insert(hash, node.id.clone());
        }
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn remove_node(&mut self, node_id: &str) -> Option<RingNode> {
        if let Some(node) = self.nodes.remove(node_id) {
            for i in 0..self.virtual_nodes {
                let virtual_key = format!("{}:vnode:{}", node_id, i);
                let hash = self.hash_key(&virtual_key);
                self.ring.remove(&hash);
            }
            Some(node)
        } else {
            None
        }
    }

    pub fn get_node<T: Hash>(&self, key: &T) -> Option<&RingNode> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash_key(key);
        let node_id = self
            .ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, id)| id)?;

        self.nodes.get(node_id)
    }

    pub fn get_nodes<T: Hash>(&self, key: &T, count: usize) -> Vec<&RingNode> {
        if self.ring.is_empty() || count == 0 {
            return Vec::new();
        }

        let hash = self.hash_key(key);
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        let primary: Vec<_> = self.ring.range(hash..).map(|(_, id)| id.as_str()).collect();
        let wrap: Vec<_> = self.ring.range(..hash).map(|(_, id)| id.as_str()).collect();

        for node_id in primary.iter().chain(wrap.iter()) {
            if !seen.insert(node_id) {
                continue;
            }
            if let Some(node) = self.nodes.get(*node_id) {
                result.push(node);
            }
            if result.len() >= count || seen.len() >= self.nodes.len() {
                break;
            }
        }

        result
    }

    pub fn get_all_nodes(&self) -> Vec<&RingNode> {
        self.nodes.values().collect()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
    }

    pub fn contains(&self, node_id: &str) -> bool {
        self.nodes.contains_key(node_id)
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new(150)
    }
}

fn parse_host_port(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (
            host.to_string(),
            port.parse().unwrap_or(0),
        ),
        None => (addr.to_string(), 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_with(ids: &[&str]) -> HashRing {
        let mut ring = HashRing::new(8);
        for (i, id) in ids.iter().enumerate() {
            ring.add_node(RingNode::new(*id, "127.0.0.1", 9000 + i as u16));
        }
        ring
    }

    #[test]
    fn add_and_lookup_node() {
        let mut ring = HashRing::new(4);
        ring.add_node(RingNode::new("a", "10.0.0.1", 9001));
        assert_eq!(ring.node_count(), 1);
        assert!(ring.get_node(&"job-1").is_some());
    }

    #[test]
    fn remove_node() {
        let mut ring = ring_with(&["a", "b"]);
        assert_eq!(ring.remove_node("a").map(|n| n.id), Some("a".into()));
        assert_eq!(ring.node_count(), 1);
        assert!(!ring.contains("a"));
    }

    #[test]
    fn same_key_same_node() {
        let ring = ring_with(&["a", "b", "c"]);
        let k = "user-42";
        let first = ring.get_node(&k).map(|n| n.id.clone());
        assert_eq!(ring.get_node(&k).map(|n| n.id.clone()), first);
    }

    #[test]
    fn get_nodes_replicas() {
        let ring = ring_with(&["a", "b", "c"]);
        let nodes = ring.get_nodes(&"order-7", 2);
        assert_eq!(nodes.len(), 2);
        assert_ne!(nodes[0].id, nodes[1].id);
    }
}

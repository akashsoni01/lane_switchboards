//! Consistent-hash ring for cluster node discovery.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Stable FNV-1a 64-bit hasher for cross-version consistent routing.
#[derive(Default)]
struct FnvHasher {
    state: u64,
}

impl FnvHasher {
    fn new() -> Self {
        Self {
            state: FNV_OFFSET_BASIS,
        }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state ^= u64::from(b);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }
}

/// Physical cluster node (id + host + port). Distinct from TCP [`crate::distributed::Node`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub fn try_from_socket_addr(id: impl Into<String>, addr: &str) -> Result<Self, String> {
        let id = id.into();
        let (host, port) = parse_host_port(addr)?;
        Ok(Self { id, host, port })
    }

    /// Parse `"host:port"`, panicking if the address is malformed.
    pub fn from_socket_addr(id: impl Into<String>, addr: &str) -> Self {
        Self::try_from_socket_addr(id, addr)
            .unwrap_or_else(|e| panic!("invalid socket address {addr:?}: {e}"))
    }

    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
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
        let mut hasher = FnvHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn add_node(&mut self, node: RingNode) {
        for i in 0..self.virtual_nodes {
            let virtual_key = format!("{}:vnode:{}", node.id, i);
            let hash = self.hash_key(&virtual_key);
            if let Some(previous) = self.ring.insert(hash, node.id.clone()) {
                tracing::warn!(
                    node = %node.id,
                    vnode = i,
                    hash,
                    previous = %previous,
                    "hash collision on ring insert — virtual node lost"
                );
            }
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
        let mut result = Vec::with_capacity(count);
        let mut seen = HashSet::new();

        for (_, node_id) in self.ring.range(hash..).chain(self.ring.range(..hash)) {
            if !seen.insert(node_id.as_str()) {
                continue;
            }
            if let Some(node) = self.nodes.get(node_id.as_str()) {
                result.push(node);
                if result.len() >= count {
                    break;
                }
            }
            if seen.len() >= self.nodes.len() {
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

fn parse_host_port(addr: &str) -> Result<(String, u16), String> {
    let (host, port_str) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("missing port in address: {addr:?}"))?;
    let port = port_str
        .parse::<u16>()
        .map_err(|_| format!("invalid port {port_str:?} in address: {addr:?}"))?;
    Ok((host.to_string(), port))
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

    #[test]
    fn parse_host_port_rejects_bad_input() {
        assert!(parse_host_port("10.0.0.1").is_err());
        assert!(parse_host_port("10.0.0.1:notaport").is_err());
        assert_eq!(
            parse_host_port("127.0.0.1:9001").unwrap(),
            ("127.0.0.1".to_string(), 9001)
        );
    }
}

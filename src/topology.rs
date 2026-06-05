//! Declarative multi-datacenter cluster topology builder.
//!
//! Collapses the repetitive setup in multi-DC examples — node naming, `serve_actor`,
//! [`ClusterMember::with_dc`], and [`Cluster::join`] — into a chainable [`DcTopology`]
//! builder and a single [`DcCluster::spawn`] call.

use crate::actor::Actor;
use crate::distributed::{serve_actor, Cluster, ClusterMember, NodeHandle, RemoteMessage};
use std::collections::HashMap;

/// Declarative datacenter layout: each entry is `(dc_name, node_count)`.
#[derive(Debug, Default, Clone)]
pub struct DcTopology {
    datacenters: Vec<(String, usize)>,
}

impl DcTopology {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a datacenter with `count` nodes. Chainable.
    pub fn datacenter(mut self, name: impl Into<String>, count: usize) -> Self {
        self.datacenters.push((name.into(), count));
        self
    }

    pub fn total_nodes(&self) -> usize {
        self.datacenters.iter().map(|(_, count)| count).sum()
    }

    pub fn datacenters(&self) -> &[(String, usize)] {
        &self.datacenters
    }
}

/// Metadata for one spawned node in a [`DcCluster`].
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub name: String,
    pub dc: String,
    pub addr: String,
}

/// A populated multi-DC [`Cluster`] with node bookkeeping.
pub struct DcCluster<M: RemoteMessage> {
    cluster: Cluster<M>,
    nodes: Vec<NodeInfo>,
    addr_to_name: HashMap<String, String>,
    _handles: Vec<NodeHandle<M>>,
}

impl<M: RemoteMessage> DcCluster<M> {
    /// Spawn every node in `topology` using `build(dc, node_name)` to create the actor,
    /// bind each via [`serve_actor`] on `127.0.0.1:0`, tag with its DC, and join
    /// a single [`Cluster`]. `target` is the gRPC deliver target (e.g. `"heartbeat"`).
    ///
    /// Nodes are named `"{dc}-{i}"` with `i` starting at 1.
    pub async fn spawn<B, A>(
        topology: DcTopology,
        target: impl Into<String>,
        build: B,
    ) -> std::io::Result<Self>
    where
        B: Fn(&str, &str) -> A,
        A: Actor<M> + Send + Sync + 'static,
    {
        let target = target.into();
        let mut cluster = Cluster::<M>::new();
        let mut nodes = Vec::with_capacity(topology.total_nodes());
        let mut addr_to_name = HashMap::with_capacity(topology.total_nodes());
        let mut handles = Vec::with_capacity(topology.total_nodes());

        for (dc, count) in topology.datacenters() {
            for i in 1..=*count {
                let name = format!("{dc}-{i}");
                let actor = build(dc, &name);
                let handle = serve_actor(&name, "127.0.0.1:0", &target, actor).await?;
                let addr = handle.address().to_string();
                let member = ClusterMember::new(&name, &addr, &target).with_dc(dc.clone());
                cluster.join(member);
                addr_to_name.insert(addr.clone(), name.clone());
                nodes.push(NodeInfo {
                    name: name.clone(),
                    dc: dc.clone(),
                    addr,
                });
                handles.push(handle);
            }
        }

        tracing::info!(
            nodes = nodes.len(),
            dcs = topology.datacenters().len(),
            "dc topology spawned"
        );

        Ok(Self {
            cluster,
            nodes,
            addr_to_name,
            _handles: handles,
        })
    }

    pub fn cluster(&self) -> &Cluster<M> {
        &self.cluster
    }

    /// All spawned nodes in spawn order (DC declaration order, then index 1..=count).
    pub fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }

    pub fn node_name(&self, addr: &str) -> Option<&str> {
        self.addr_to_name.get(addr).map(String::as_str)
    }

    pub fn dc_node_names(&self, dc: &str) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|node| node.dc == dc)
            .map(|node| node.name.as_str())
            .collect()
    }

    /// Send `msg` to every node in `dc` (wraps [`Cluster::dc_members`]).
    pub async fn broadcast_to_dc(&self, dc: &str, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        let mut results = Vec::new();
        for remote in self.cluster.dc_members(dc) {
            let name = self
                .nodes
                .iter()
                .find(|node| node.addr == remote.node_addr)
                .map(|node| node.name.clone())
                .unwrap_or_else(|| remote.node_addr.clone());
            results.push((name, remote.send(msg.clone()).await));
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, ActorProcessingErr};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[derive(Clone, PartialEq, prost::Message)]
    struct PingMsg {
        #[prost(uint64, tag = "1")]
        n: u64,
    }

    struct PingCounter(Arc<AtomicU64>);

    #[async_trait::async_trait]
    impl Actor<PingMsg> for PingCounter {
        async fn handle(&mut self, msg: PingMsg) -> Result<(), ActorProcessingErr> {
            self.0.fetch_add(msg.n, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn dc_topology_counts_and_node_names() {
        let topology = DcTopology::new()
            .datacenter("east", 2)
            .datacenter("west", 2);

        assert_eq!(topology.total_nodes(), 4);
        assert_eq!(topology.datacenters().len(), 2);

        let dc = DcCluster::spawn(topology, "worker", |_dc, _name| {
            PingCounter(Arc::new(AtomicU64::new(0)))
        })
        .await
        .expect("spawn");

        assert_eq!(dc.nodes().len(), 4);
        assert_eq!(dc.cluster().len(), 4);
        assert_eq!(dc.dc_node_names("east"), vec!["east-1", "east-2"]);
        assert_eq!(dc.dc_node_names("west"), vec!["west-1", "west-2"]);
        assert_eq!(dc.cluster().dc_members("east").len(), 2);
        assert_eq!(dc.cluster().dc_members("west").len(), 2);

        for node in dc.nodes() {
            assert_eq!(dc.node_name(&node.addr), Some(node.name.as_str()));
        }
    }

    #[tokio::test]
    async fn broadcast_to_dc_reaches_all_members() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_c = counter.clone();
        let dc = DcCluster::spawn(
            DcTopology::new().datacenter("alpha", 2),
            "worker",
            move |_dc, _name| PingCounter(counter_c.clone()),
        )
        .await
        .expect("spawn");

        let results = dc.broadcast_to_dc("alpha", PingMsg { n: 1 }).await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(_, r)| r.is_ok()));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}

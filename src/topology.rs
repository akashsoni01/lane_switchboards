//! Declarative multi-datacenter cluster topology builder.
//!
//! Collapses the repetitive setup in multi-DC examples — node naming, `serve_actor`,
//! [`ClusterMember::with_dc`], and [`Cluster::join`] — into a chainable [`DcTopology`]
//! builder and [`DcCluster`] / [`DcWorkers`] spawn helpers.

use crate::actor::Actor;
use crate::distributed::{serve_actor, Cluster, ClusterMember, NodeHandle, RemoteMessage};
use std::collections::HashMap;

/// One datacenter in a [`DcTopology`].
#[derive(Debug, Clone)]
pub struct DatacenterSpec {
    pub name: String,
    pub count: usize,
    /// When set, node `i` binds `127.0.0.1:{port_base + i}`.
    pub port_base: Option<u16>,
}

/// Declarative datacenter layout.
#[derive(Debug, Default, Clone)]
pub struct DcTopology {
    datacenters: Vec<DatacenterSpec>,
}

impl DcTopology {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a datacenter with `count` nodes on ephemeral ports. Chainable.
    pub fn datacenter(mut self, name: impl Into<String>, count: usize) -> Self {
        self.datacenters.push(DatacenterSpec {
            name: name.into(),
            count,
            port_base: None,
        });
        self
    }

    /// Add a datacenter whose nodes bind `127.0.0.1:{port_base + i}` for `i` in `1..=count`.
    ///
    /// Reserve `port_base` for a regional gateway in multi-DC production layouts.
    pub fn datacenter_with_ports(
        mut self,
        name: impl Into<String>,
        count: usize,
        port_base: u16,
    ) -> Self {
        self.datacenters.push(DatacenterSpec {
            name: name.into(),
            count,
            port_base: Some(port_base),
        });
        self
    }

    pub fn total_nodes(&self) -> usize {
        self.datacenters.iter().map(|spec| spec.count).sum()
    }

    pub fn specs(&self) -> &[DatacenterSpec] {
        &self.datacenters
    }

    /// `(dc_name, node_count)` pairs — port bases omitted.
    pub fn datacenters(&self) -> Vec<(String, usize)> {
        self.datacenters
            .iter()
            .map(|spec| (spec.name.clone(), spec.count))
            .collect()
    }

    fn bind_addr(spec: &DatacenterSpec, index: u16) -> String {
        match spec.port_base {
            Some(base) => format!("127.0.0.1:{}", base + index),
            None => "127.0.0.1:0".to_string(),
        }
    }
}

/// Metadata for one spawned node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub name: String,
    pub dc: String,
    pub addr: String,
}

struct SpawnedNodes<M: RemoteMessage> {
    nodes: Vec<NodeInfo>,
    addr_to_name: HashMap<String, String>,
    handles: Vec<NodeHandle<M>>,
}

async fn spawn_topology_nodes<M, B, A>(
    topology: &DcTopology,
    target: &str,
    build: &B,
) -> std::io::Result<SpawnedNodes<M>>
where
    M: RemoteMessage,
    B: Fn(&str, &str) -> A,
    A: Actor<M> + Send + Sync + 'static,
{
    let mut nodes = Vec::with_capacity(topology.total_nodes());
    let mut addr_to_name = HashMap::with_capacity(topology.total_nodes());
    let mut handles = Vec::with_capacity(topology.total_nodes());

    for spec in topology.specs() {
        for i in 1..=spec.count {
            let name = format!("{}-{i}", spec.name);
            let actor = build(&spec.name, &name);
            let bind = DcTopology::bind_addr(spec, i as u16);
            let handle = serve_actor(&name, &bind, target, actor).await?;
            let addr = handle.address().to_string();
            addr_to_name.insert(addr.clone(), name.clone());
            nodes.push(NodeInfo {
                name,
                dc: spec.name.clone(),
                addr,
            });
            handles.push(handle);
        }
    }

    Ok(SpawnedNodes {
        nodes,
        addr_to_name,
        handles,
    })
}

/// Worker pool for one or more datacenters — no [`Cluster`] join (regional site).
pub struct DcWorkers<M: RemoteMessage> {
    nodes: Vec<NodeInfo>,
    addr_to_name: HashMap<String, String>,
    _handles: Vec<NodeHandle<M>>,
}

impl<M: RemoteMessage> DcWorkers<M> {
    /// Spawn nodes declared in `topology` without joining a cluster.
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
        let spawned = spawn_topology_nodes(&topology, &target, &build).await?;
        tracing::info!(
            nodes = spawned.nodes.len(),
            dcs = topology.specs().len(),
            "dc workers spawned"
        );
        Ok(Self {
            nodes: spawned.nodes,
            addr_to_name: spawned.addr_to_name,
            _handles: spawned.handles,
        })
    }

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
}

/// A populated multi-DC [`Cluster`] with node bookkeeping.
pub struct DcCluster<M: RemoteMessage> {
    cluster: Cluster<M>,
    nodes: Vec<NodeInfo>,
    addr_to_name: HashMap<String, String>,
    _handles: Vec<NodeHandle<M>>,
}

impl<M: RemoteMessage> DcCluster<M> {
    /// Spawn every node in `topology`, tag with its DC, and join one [`Cluster`].
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
        let spawned = spawn_topology_nodes(&topology, &target, &build).await?;
        let mut cluster = Cluster::<M>::new();
        for node in &spawned.nodes {
            let member =
                ClusterMember::new(&node.name, &node.addr, &target).with_dc(node.dc.clone());
            cluster.join(member);
        }

        tracing::info!(
            nodes = spawned.nodes.len(),
            dcs = topology.specs().len(),
            "dc topology spawned"
        );

        Ok(Self {
            cluster,
            nodes: spawned.nodes,
            addr_to_name: spawned.addr_to_name,
            _handles: spawned.handles,
        })
    }

    pub fn cluster(&self) -> &Cluster<M> {
        &self.cluster
    }

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
    async fn datacenter_with_ports_binds_expected_addresses() {
        let workers = DcWorkers::spawn(
            DcTopology::new().datacenter_with_ports("east", 2, 29_100),
            "worker",
            |_dc, _name| PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("spawn");

        assert_eq!(workers.nodes()[0].addr, "127.0.0.1:29101");
        assert_eq!(workers.nodes()[1].addr, "127.0.0.1:29102");
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

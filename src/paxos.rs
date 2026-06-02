//! Single-key Paxos acceptors for [`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`].
//!
//! Implements Prepare → Promise (read path). Propose → Accept → Commit (CAS writes) are
//! defined on the wire but client-side conditional writes are not implemented yet.

use crate::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use crate::config::DistributedConfig;
use crate::consistency::ConsistencyError;
use crate::distributed::{
    paxos_request, Node, PaxosRpc,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

static BALLOT_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_ballot() -> u64 {
    BALLOT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// TCP dispatch target for a service Paxos acceptor: `__paxos__{service}`.
pub fn paxos_target(service: &str) -> String {
    format!("__paxos__{service}")
}

/// Prepare phase message for Paxos reads ([`ReadConsistency::Serial`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Prepare {
    pub ballot: u64,
    pub key: String,
}

/// Acceptor response to [`Prepare`] carrying the highest accepted value, if any.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Promise {
    pub ballot: u64,
    pub accepted: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Propose {
    pub ballot: u64,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Accept {
    pub ballot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reject {
    pub ballot: u64,
    pub higher: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    pub ballot: u64,
    pub key: String,
    pub value: Vec<u8>,
}

/// Length-prefixed JSON Paxos wire messages (same framing as data-plane frames).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PaxosWireMsg {
    Prepare(Prepare),
    Promise(Promise),
    Propose(Propose),
    Accept(Accept),
    Reject(Reject),
    Commit(Commit),
}

#[derive(Default)]
struct KeyState {
    highest_promised: u64,
    accepted: Option<(u64, Vec<u8>)>,
}

/// Per-key Paxos acceptor state machine.
#[derive(Default)]
pub struct PaxosAcceptor {
    keys: HashMap<String, KeyState>,
}

impl PaxosAcceptor {
    pub fn handle(&mut self, msg: PaxosWireMsg) -> PaxosWireMsg {
        match msg {
            PaxosWireMsg::Prepare(p) => self.prepare(p),
            PaxosWireMsg::Propose(prop) => self.propose(prop),
            _ => PaxosWireMsg::Reject(Reject {
                ballot: 0,
                higher: 0,
            }),
        }
    }

    fn prepare(&mut self, p: Prepare) -> PaxosWireMsg {
        let state = self.keys.entry(p.key).or_default();
        if p.ballot >= state.highest_promised {
            state.highest_promised = p.ballot;
            PaxosWireMsg::Promise(Promise {
                ballot: p.ballot,
                accepted: state.accepted.clone(),
            })
        } else {
            PaxosWireMsg::Reject(Reject {
                ballot: p.ballot,
                higher: state.highest_promised,
            })
        }
    }

    fn propose(&mut self, p: Propose) -> PaxosWireMsg {
        let state = self.keys.entry(p.key).or_default();
        if p.ballot >= state.highest_promised {
            state.highest_promised = p.ballot;
            state.accepted = Some((p.ballot, p.value.clone()));
            PaxosWireMsg::Accept(Accept { ballot: p.ballot })
        } else {
            PaxosWireMsg::Reject(Reject {
                ballot: p.ballot,
                higher: state.highest_promised,
            })
        }
    }

    /// Test / admin helper: inject an accepted value for a key.
    pub fn inject_accepted(&mut self, key: impl Into<String>, ballot: u64, value: Vec<u8>) {
        let state = self.keys.entry(key.into()).or_default();
        state.highest_promised = state.highest_promised.max(ballot);
        state.accepted = Some((ballot, value));
    }
}

/// Actor messages for [`PaxosNode`].
pub enum PaxosMsg {
    Rpc(PaxosWireMsg, oneshot::Sender<PaxosWireMsg>),
    Inject {
        key: String,
        ballot: u64,
        value: Vec<u8>,
        done: Option<oneshot::Sender<()>>,
    },
}

/// Paxos acceptor actor — one instance per service replica.
#[derive(Default)]
pub struct PaxosNode {
    acceptor: PaxosAcceptor,
}

#[async_trait::async_trait]
impl Actor<PaxosMsg> for PaxosNode {
    async fn handle(&mut self, msg: PaxosMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            PaxosMsg::Rpc(wire, reply) => {
                let resp = self.acceptor.handle(wire);
                let _ = reply.send(resp);
            }
            PaxosMsg::Inject { key, ballot, value, done } => {
                self.acceptor.inject_accepted(key, ballot, value);
                if let Some(done) = done {
                    let _ = done.send(());
                }
            }
        }
        Ok(())
    }
}

/// Handle returned when a Paxos acceptor is bound to TCP.
pub struct PaxosHandle {
    pub target: String,
    pub address: String,
    pub actor: ActorRef<PaxosMsg>,
    _bridge: tokio::task::JoinHandle<()>,
    _node: Node<PaxosPlaceholder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaxosPlaceholder;

/// A remote or local Paxos replica endpoint.
#[derive(Clone)]
pub struct PaxosReplica {
    pub node_addr: String,
    pub target: String,
}

impl PaxosReplica {
    pub fn from_member(service: &str, node_addr: impl Into<String>) -> Self {
        Self {
            node_addr: node_addr.into(),
            target: paxos_target(service),
        }
    }
}

/// Client-side Paxos proposer (read / prepare phase).
#[derive(Default)]
pub struct PaxosProposer {
    config: DistributedConfig,
}

impl PaxosProposer {
    pub fn new(config: DistributedConfig) -> Self {
        Self { config }
    }

    /// Run Prepare against a quorum and return the highest accepted value, if any.
    ///
    /// Used by [`crate::ServiceMesh::read_serial_value`] for
    /// [`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lane_switchboards::{PaxosProposer, PaxosReplica};
    /// # use std::time::Duration;
    /// # async fn example(replicas: &[PaxosReplica]) -> Result<Option<Vec<u8>>, lane_switchboards::ConsistencyError> {
    /// PaxosProposer::default().read("my-key", replicas, 2, Duration::from_secs(5)).await
    /// # }
    /// ```
    pub async fn read(
        &self,
        key: &str,
        replicas: &[PaxosReplica],
        quorum: usize,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>, ConsistencyError> {
        if replicas.len() < quorum {
            return Err(ConsistencyError::NotEnoughReplicas {
                required: quorum,
                available: replicas.len(),
            });
        }

        const MAX_ROUNDS: usize = 3;
        let mut ballot = next_ballot();

        for round in 0..MAX_ROUNDS {
            let prepare = PaxosWireMsg::Prepare(Prepare {
                ballot,
                key: key.to_string(),
            });

            let mut join_set = tokio::task::JoinSet::new();
            for replica in replicas {
                let rep = replica.clone();
                let prep = prepare.clone();
                let config = self.config;
                join_set.spawn(async move { send_prepare(&rep, &prep, timeout, &config).await });
            }

            let collect = tokio::time::timeout(timeout, async {
                let mut promises = Vec::new();
                let mut saw_reject = false;
                while let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(PaxosWireMsg::Promise(p))) => promises.push(p),
                        Ok(Ok(PaxosWireMsg::Reject(_))) => saw_reject = true,
                        Ok(Ok(_)) => {}
                        Ok(Err(_)) | Err(_) => {}
                    }
                }
                (promises, saw_reject)
            })
            .await;

            join_set.abort_all();

            match collect {
                Ok((promises, _saw_reject)) if promises.len() >= quorum => {
                    let mut highest: Option<(u64, Vec<u8>)> = None;
                    for p in promises {
                        if let Some((b, ref v)) = p.accepted {
                            if highest.as_ref().map(|(hb, _)| b > *hb).unwrap_or(true) {
                                highest = Some((b, v.clone()));
                            }
                        }
                    }
                    return Ok(highest.map(|(_, v)| v));
                }
                Ok((_, saw_reject)) if saw_reject && round + 1 < MAX_ROUNDS => {
                    ballot = next_ballot();
                    continue;
                }
                Ok((promises, _)) => {
                    return Err(ConsistencyError::NotEnoughAcks {
                        required: quorum,
                        received: promises.len(),
                        dc: None,
                    });
                }
                Err(_) => return Err(ConsistencyError::Timeout { after: timeout }),
            }
        }

        Err(ConsistencyError::PaxosContention { rounds: MAX_ROUNDS })
    }
}

async fn send_prepare(
    replica: &PaxosReplica,
    prepare: &PaxosWireMsg,
    timeout: Duration,
    config: &DistributedConfig,
) -> Result<PaxosWireMsg, ConsistencyError> {
    let payload = serde_json::to_value(prepare).map_err(|_| ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    })?;
    let data = paxos_request(
        &replica.node_addr,
        &replica.target,
        payload,
        timeout,
        config,
        None,
    )
    .await?;
    serde_json::from_value(data).map_err(|_| ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    })
}

/// Spawn a Paxos acceptor on TCP and register it under [`paxos_target`].
pub async fn serve_paxos_acceptor(
    service: impl Into<String>,
    bind_addr: impl Into<String>,
) -> std::io::Result<PaxosHandle> {
    serve_paxos_acceptor_on_runtime(&Handle::current(), service, bind_addr).await
}

/// Spawn a Paxos acceptor on a dedicated runtime handle.
pub async fn serve_paxos_acceptor_on_runtime(
    runtime: &Handle,
    service: impl Into<String>,
    bind_addr: impl Into<String>,
) -> std::io::Result<PaxosHandle> {
    let service = service.into();
    let target = paxos_target(&service);
    let distributed = DistributedConfig::default();

    let (actor, _join) = spawn(PaxosNode::default(), None).await.map_err(|e| {
        std::io::Error::other(format!("spawn paxos node: {e}"))
    })?;

    let node = Node::<PaxosPlaceholder>::bind_on_runtime(
        runtime,
        format!("paxos-{service}"),
        bind_addr,
        &distributed,
        None,
    )
    .await?;

    let address = node.address().to_string();
    let (rpc_tx, mut rpc_rx) = mpsc::channel(distributed.bridge_capacity);
    node.register_paxos(&target, rpc_tx).await;

    let actor_ref = actor.clone();
    let paxos_table = node.paxos_dispatch();
    let target_for_bridge = target.clone();
    let bridge = tokio::spawn(async move {
        while let Some(PaxosRpc { payload, reply }) = rpc_rx.recv().await {
            let result = async {
                let wire: PaxosWireMsg = serde_json::from_value(payload)
                    .map_err(|e| format!("invalid paxos payload: {e}"))?;
                let (resp_tx, resp_rx) = oneshot::channel();
                actor_ref
                    .send(PaxosMsg::Rpc(wire, resp_tx))
                    .await
                    .map_err(|_| "paxos actor mailbox closed".to_string())?;
                let resp = resp_rx
                    .await
                    .map_err(|_| "paxos actor dropped reply".to_string())?;
                serde_json::to_value(resp).map_err(|e| format!("encode paxos reply: {e}"))
            }
            .await;
            let _ = reply.send(result);
        }
        paxos_table.lock().await.remove(&target_for_bridge);
    });

    Ok(PaxosHandle {
        target,
        address,
        actor,
        _bridge: bridge,
        _node: node,
    })
}

// TODO: Client-side CAS writes (Propose/Accept from proposer) — out of scope for this pass.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    fn replicas_for(handles: &[PaxosHandle], service: &str) -> Vec<PaxosReplica> {
        handles
            .iter()
            .map(|h| PaxosReplica::from_member(service, &h.address))
            .collect()
    }

    #[tokio::test]
    async fn paxos_rpc_roundtrip() {
        let h = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("serve");
        let prepare = PaxosWireMsg::Prepare(Prepare {
            ballot: 1,
            key: "k".into(),
        });
        let payload = serde_json::to_value(&prepare).unwrap();
        let resp = crate::distributed::paxos_request(
            &h.address,
            &h.target,
            payload,
            Duration::from_secs(2),
            &DistributedConfig::default(),
            None,
        )
        .await
        .expect("rpc");
        let wire: PaxosWireMsg = serde_json::from_value(resp).expect("parse");
        assert!(matches!(wire, PaxosWireMsg::Promise(_)));
    }

    #[tokio::test]
    async fn paxos_read_empty_state() {
        let h1 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h1");
        let h2 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h2");
        let h3 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h3");
        let replicas = replicas_for(&[h1, h2, h3], "kv");

        let value = PaxosProposer::default()
            .read("user:1", &replicas, 2, Duration::from_secs(2))
            .await
            .expect("read");
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn paxos_read_highest_accepted_wins() {
        let h1 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h1");
        let h2 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h2");
        let h3 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h3");

        let (done_tx, done_rx) = oneshot::channel();
        h2.actor
            .send(PaxosMsg::Inject {
                key: "user:1".into(),
                ballot: 1,
                value: b"hello".to_vec(),
                done: Some(done_tx),
            })
            .await
            .expect("inject");
        done_rx.await.expect("inject applied");

        let replicas = replicas_for(&[h1, h2, h3], "kv");
        let value = PaxosProposer::default()
            .read("user:1", &replicas, 2, Duration::from_secs(2))
            .await
            .expect("read");
        assert_eq!(value, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn paxos_concurrent_proposers_contend_or_succeed() {
        let h1 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h1");
        let h2 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h2");
        let h3 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h3");
        let replicas = Arc::new(replicas_for(&[h1, h2, h3], "kv"));
        let barrier = Arc::new(Barrier::new(2));

        let r1 = replicas.clone();
        let b1 = barrier.clone();
        let t1 = tokio::spawn(async move {
            b1.wait().await;
            PaxosProposer::default()
                .read("hot-key", &r1, 2, Duration::from_secs(3))
                .await
        });

        let r2 = replicas.clone();
        let b2 = barrier.clone();
        let t2 = tokio::spawn(async move {
            b2.wait().await;
            PaxosProposer::default()
                .read("hot-key", &r2, 2, Duration::from_secs(3))
                .await
        });

        let (o1, o2) = tokio::join!(t1, t2);
        let r1 = o1.expect("join1");
        let r2 = o2.expect("join2");

        let ok = r1.is_ok() || r2.is_ok();
        let contention = matches!(r1, Err(ConsistencyError::PaxosContention { .. }))
            || matches!(r2, Err(ConsistencyError::PaxosContention { .. }));
        assert!(ok || contention);
    }

    #[test]
    fn acceptor_prepare_promise_and_reject() {
        let mut acc = PaxosAcceptor::default();
        let p1 = Prepare {
            ballot: 1,
            key: "k".into(),
        };
        match acc.handle(PaxosWireMsg::Prepare(p1.clone())) {
            PaxosWireMsg::Promise(p) => {
                assert_eq!(p.ballot, 1);
                assert!(p.accepted.is_none());
            }
            _ => panic!("expected promise"),
        }

        let p2 = Prepare {
            ballot: 0,
            key: "k".into(),
        };
        assert!(matches!(
            acc.handle(PaxosWireMsg::Prepare(p2)),
            PaxosWireMsg::Reject(Reject { higher: 1, .. })
        ));
    }
}

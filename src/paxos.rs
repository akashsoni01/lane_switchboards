//! Single-key Paxos acceptors for [`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`].
//!
//! Implements Prepare → Promise (read path). Propose → Accept → Commit (CAS writes) are
//! defined on the wire but client-side conditional writes are not implemented yet.

use crate::actor::{Actor, ActorProcessingErr, ActorRef};
use crate::config::DistributedConfig;
use crate::consistency::ConsistencyError;
use crate::paxos_grpc::{bind_paxos_acceptor_on_runtime, PaxosAcceptorHandle};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

static BALLOT_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_ballot() -> u64 {
    BALLOT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Legacy dispatch label for a service Paxos acceptor: `__paxos__{service}` (gRPC uses dedicated acceptor servers).
pub fn paxos_target(service: &str) -> String {
    format!("__paxos__{service}")
}

/// Prepare phase message for Paxos reads ([`ReadConsistency::Serial`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    pub ballot: u64,
    pub key: String,
}

/// Acceptor response to [`Prepare`] carrying the highest accepted value, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promise {
    pub ballot: u64,
    pub accepted: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Propose {
    pub ballot: u64,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accept {
    pub ballot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    pub ballot: u64,
    pub higher: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub ballot: u64,
    pub key: String,
    pub value: Vec<u8>,
}

/// In-process Paxos messages handled by [`PaxosNode`] (gRPC uses `proto::paxos` on the wire).
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Handle returned when a Paxos acceptor is bound (gRPC).
pub struct PaxosHandle {
    pub target: String,
    pub address: String,
    pub actor: ActorRef<PaxosMsg>,
    _inner: PaxosAcceptorHandle,
}

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
                let config = self.config.clone();
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
    _config: &DistributedConfig,
) -> Result<PaxosWireMsg, ConsistencyError> {
    let PaxosWireMsg::Prepare(p) = prepare else {
        return Err(ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        });
    };
    let uri = crate::distributed::grpc_data_endpoint(&replica.node_addr, false);
    let mut client =
        crate::proto::paxos::paxos_acceptor_client::PaxosAcceptorClient::connect(uri)
            .await
            .map_err(|_| ConsistencyError::NotEnoughAcks {
                required: 1,
                received: 0,
                dc: None,
            })?;
    let reply = tokio::time::timeout(
        timeout,
        client.prepare(crate::proto::paxos::PrepareRequest {
            ballot: p.ballot,
            key: p.key.clone(),
        }),
    )
    .await
    .map_err(|_| ConsistencyError::Timeout { after: timeout })?
    .map_err(|_| ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    })?
    .into_inner();
    if reply.promised {
        let accepted = if reply.accepted_ballot > 0 {
            Some((reply.accepted_ballot, reply.accepted_value))
        } else {
            None
        };
        Ok(PaxosWireMsg::Promise(Promise {
            ballot: reply.ballot,
            accepted,
        }))
    } else {
        Ok(PaxosWireMsg::Reject(Reject {
            ballot: p.ballot,
            higher: reply.accepted_ballot,
        }))
    }
}

/// Spawn a Paxos acceptor (gRPC) for a service.
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
    let inner = bind_paxos_acceptor_on_runtime(runtime, service, bind_addr).await?;
    Ok(PaxosHandle {
        target: inner.target.clone(),
        address: inner.address.clone(),
        actor: inner.actor.clone(),
        _inner: inner,
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
    async fn paxos_grpc_prepare_roundtrip() {
        let h = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("serve");
        let prepare = PaxosWireMsg::Prepare(Prepare {
            ballot: 1,
            key: "k".into(),
        });
        let wire = send_prepare(
            &PaxosReplica::from_member("kv", &h.address),
            &prepare,
            Duration::from_secs(2),
            &DistributedConfig::default(),
        )
        .await
        .expect("grpc prepare");
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

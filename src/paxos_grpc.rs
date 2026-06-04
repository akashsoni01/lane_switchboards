//! gRPC Paxos acceptor service (tonic).

use crate::paxos::{PaxosMsg, PaxosNode, PaxosWireMsg, Prepare, Propose};
use crate::proto::paxos::paxos_acceptor_server::PaxosAcceptor;
use crate::proto::paxos::{
    AcceptReply, Ack, CommitRequest, PrepareRequest, PromiseReply, ProposeRequest,
};
use crate::actor::{spawn_on_runtime, ActorRef};
use crate::config::ActorConfig;
use std::net::SocketAddr;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

/// Running Paxos acceptor (gRPC).
pub struct PaxosAcceptorHandle {
    pub target: String,
    pub address: String,
    pub actor: ActorRef<PaxosMsg>,
    _task: JoinHandle<()>,
}

async fn dispatch_wire(
    actor: &ActorRef<PaxosMsg>,
    wire: PaxosWireMsg,
) -> Result<PaxosWireMsg, Status> {
    let (tx, rx) = oneshot::channel();
    actor
        .send(PaxosMsg::Rpc(wire, tx))
        .await
        .map_err(|e| Status::internal(format!("paxos actor: {e}")))?;
    rx.await
        .map_err(|_| Status::internal("paxos actor dropped reply"))
}

#[derive(Clone)]
struct PaxosGrpcService {
    actor: ActorRef<PaxosMsg>,
}

#[tonic::async_trait]
impl PaxosAcceptor for PaxosGrpcService {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PromiseReply>, Status> {
        let req = request.into_inner();
        let wire = PaxosWireMsg::Prepare(Prepare {
            ballot: req.ballot,
            key: req.key,
        });
        let resp = dispatch_wire(&self.actor, wire).await?;
        match resp {
            PaxosWireMsg::Promise(p) => {
                let (accepted_ballot, accepted_value) = match p.accepted {
                    Some((b, v)) => (b, v),
                    None => (0, Vec::new()),
                };
                Ok(Response::new(PromiseReply {
                    ballot: p.ballot,
                    promised: true,
                    accepted_ballot,
                    accepted_value,
                }))
            }
            PaxosWireMsg::Reject(r) => Ok(Response::new(PromiseReply {
                ballot: r.ballot,
                promised: false,
                accepted_ballot: r.higher,
                accepted_value: Vec::new(),
            })),
            _ => Err(Status::internal("unexpected paxos prepare response")),
        }
    }

    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<AcceptReply>, Status> {
        let req = request.into_inner();
        let wire = PaxosWireMsg::Propose(Propose {
            ballot: req.ballot,
            key: req.key,
            value: req.value,
        });
        let resp = dispatch_wire(&self.actor, wire).await?;
        match resp {
            PaxosWireMsg::Accept(a) => Ok(Response::new(AcceptReply {
                accepted: true,
                higher_ballot: a.ballot,
            })),
            PaxosWireMsg::Reject(r) => Ok(Response::new(AcceptReply {
                accepted: false,
                higher_ballot: r.higher,
            })),
            _ => Err(Status::internal("unexpected paxos propose response")),
        }
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let (done_tx, done_rx) = oneshot::channel();
        self.actor
            .send(PaxosMsg::Inject {
                key: req.key,
                ballot: req.ballot,
                value: req.value,
                done: Some(done_tx),
            })
            .await
            .map_err(|e| Status::internal(format!("paxos inject: {e}")))?;
        done_rx
            .await
            .map_err(|_| Status::internal("paxos inject dropped"))?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }
}

/// Bind a gRPC Paxos acceptor on `addr` (`:0` for tests).
pub async fn bind_paxos_acceptor_on_runtime(
    runtime: &Handle,
    service: impl Into<String>,
    addr: impl Into<String>,
) -> std::io::Result<PaxosAcceptorHandle> {
    let service = service.into();
    let target = crate::paxos::paxos_target(&service);
    let addr_str = addr.into();
    let socket_addr: SocketAddr = addr_str.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid paxos bind address {addr_str}: {e}"),
        )
    })?;

    let (actor, _join) =
        spawn_on_runtime(runtime, PaxosNode::default(), None, &ActorConfig::default())
        .await
        .map_err(|e| std::io::Error::other(format!("spawn paxos: {e}")))?;

    let svc = PaxosGrpcService { actor: actor.clone() };
    let grpc = crate::proto::paxos::paxos_acceptor_server::PaxosAcceptorServer::new(svc);

    let listener = tokio::net::TcpListener::bind(socket_addr).await?;
    let address = listener.local_addr()?.to_string();

    let task = tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(grpc)
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %e, "paxos gRPC server exited");
        }
    });

    Ok(PaxosAcceptorHandle {
        target,
        address,
        actor,
        _task: task,
    })
}

/// gRPC client for Paxos reads / commits across acceptors.
pub struct PaxosProposerClient {
    clients: Vec<crate::proto::paxos::paxos_acceptor_client::PaxosAcceptorClient<tonic::transport::Channel>>,
}

impl PaxosProposerClient {
    pub async fn connect(addrs: &[&str]) -> Result<Self, tonic::transport::Error> {
        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let uri = crate::distributed::grpc_data_endpoint(addr, false);
            let client =
                crate::proto::paxos::paxos_acceptor_client::PaxosAcceptorClient::connect(uri)
                    .await?;
            clients.push(client);
        }
        Ok(Self { clients })
    }

    /// Prepare quorum read — returns highest accepted value bytes if any.
    pub async fn read(
        &mut self,
        key: &str,
        quorum: usize,
        timeout: std::time::Duration,
    ) -> Result<Option<Vec<u8>>, crate::consistency::ConsistencyError> {
        use crate::consistency::ConsistencyError;
        if self.clients.len() < quorum {
            return Err(ConsistencyError::NotEnoughReplicas {
                required: quorum,
                available: self.clients.len(),
            });
        }

        let ballot = 1u64;
        let key = key.to_string();
        let mut join_set = tokio::task::JoinSet::new();
        for mut client in self.clients.clone() {
            let key = key.clone();
            join_set.spawn(async move {
                client
                    .prepare(PrepareRequest {
                        ballot,
                        key,
                    })
                    .await
            });
        }

        let collected = tokio::time::timeout(timeout, async {
            let mut promises = Vec::new();
            while let Some(result) = join_set.join_next().await {
                if let Ok(Ok(resp)) = result {
                    let inner = resp.into_inner();
                    if inner.promised {
                        promises.push(inner);
                    }
                }
            }
            promises
        })
        .await;

        join_set.abort_all();

        let promises = match collected {
            Ok(p) if p.len() >= quorum => p,
            Ok(p) => {
                return Err(ConsistencyError::NotEnoughAcks {
                    required: quorum,
                    received: p.len(),
                    dc: None,
                });
            }
            Err(_) => return Err(ConsistencyError::Timeout { after: timeout }),
        };

        let mut highest: Option<(u64, Vec<u8>)> = None;
        for p in promises {
            if p.accepted_ballot > 0
                && highest
                    .as_ref()
                    .map(|(b, _)| p.accepted_ballot > *b)
                    .unwrap_or(true)
            {
                highest = Some((p.accepted_ballot, p.accepted_value));
            }
        }
        Ok(highest.map(|(_, v)| v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paxos::serve_paxos_acceptor;

    #[tokio::test]
    async fn proposer_read_after_grpc_commit() {
        let h1 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h1");
        let h2 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h2");
        let h3 = serve_paxos_acceptor("kv", "127.0.0.1:0").await.expect("h3");
        let addrs = [
            h1.address.as_str(),
            h2.address.as_str(),
            h3.address.as_str(),
        ];

        let mut client = PaxosProposerClient::connect(&addrs)
            .await
            .expect("connect");
        let empty = client
            .read("key", 2, std::time::Duration::from_secs(2))
            .await
            .expect("read empty");
        assert!(empty.is_none());

        let mut c1 =
            crate::proto::paxos::paxos_acceptor_client::PaxosAcceptorClient::connect(
                crate::distributed::grpc_data_endpoint(&h1.address, false),
            )
            .await
            .expect("c1");
        let mut c2 =
            crate::proto::paxos::paxos_acceptor_client::PaxosAcceptorClient::connect(
                crate::distributed::grpc_data_endpoint(&h2.address, false),
            )
            .await
            .expect("c2");
        c1.commit(CommitRequest {
            ballot: 1,
            key: "key".into(),
            value: b"v".to_vec(),
        })
        .await
        .expect("commit1");
        c2.commit(CommitRequest {
            ballot: 1,
            key: "key".into(),
            value: b"v".to_vec(),
        })
        .await
        .expect("commit2");

        let value = client
            .read("key", 2, std::time::Duration::from_secs(2))
            .await
            .expect("read value");
        assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    }
}

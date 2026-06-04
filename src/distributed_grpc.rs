//! gRPC data plane — [`crate::proto::data::ActorMessaging`] server.

use crate::actor::ActorRef;
use crate::distributed::RemoteMessage;
use crate::proto::data::actor_messaging_server::ActorMessaging;
use crate::proto::data::{DeliverReply, DeliverRequest};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

/// Local dispatch target for an actor name.
#[derive(Clone)]
pub enum DispatchTarget<M: RemoteMessage> {
    Actor(ActorRef<M>),
    Mailbox(mpsc::Sender<M>),
}

/// gRPC actor message delivery service.
#[derive(Clone)]
pub struct ActorMessagingService<M: RemoteMessage> {
    dispatch: Arc<Mutex<HashMap<String, DispatchTarget<M>>>>,
}

impl<M: RemoteMessage> ActorMessagingService<M> {
    pub fn new(dispatch: Arc<Mutex<HashMap<String, DispatchTarget<M>>>>) -> Self {
        Self { dispatch }
    }
}

#[tonic::async_trait]
impl<M: RemoteMessage> ActorMessaging for ActorMessagingService<M> {
    type DeliverStream =
        Pin<Box<dyn Stream<Item = Result<DeliverReply, Status>> + Send + 'static>>;

    async fn deliver(
        &self,
        request: Request<tonic::Streaming<DeliverRequest>>,
    ) -> Result<Response<Self::DeliverStream>, Status> {
        let mut inbound = request.into_inner();
        let (reply_tx, reply_rx) = mpsc::channel(64);
        let dispatch = self.dispatch.clone();

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                let req = match item {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(error = %e, "deliver stream read error");
                        break;
                    }
                };

                let span = tracing::info_span!(
                    "grpc.deliver",
                    target = %req.target,
                    frame_id = req.frame_id,
                    payload_bytes = req.payload.len(),
                );
                let _guard = span.enter();

                let decoded = match M::decode(req.payload.as_slice()) {
                    Ok(m) => m,
                    Err(e) => {
                        if req.expect_ack {
                            let _ = reply_tx
                                .send(DeliverReply {
                                    frame_id: req.frame_id,
                                    ok: false,
                                    error: format!("decode: {e}"),
                                })
                                .await;
                        }
                        continue;
                    }
                };

                let entry = {
                    let table = dispatch.lock().await;
                    table.get(&req.target).cloned()
                };

                let send_result = if let Some(entry) = entry {
                    match entry {
                        DispatchTarget::Actor(actor) => actor
                            .send(decoded)
                            .await
                            .map(|_| ())
                            .map_err(|e| format!("actor mailbox: {e}")),
                        DispatchTarget::Mailbox(tx) => tx
                            .send(decoded)
                            .await
                            .map(|_| ())
                            .map_err(|_| "mailbox closed".to_string()),
                    }
                } else {
                    tracing::warn!(target = %req.target, "no local actor for deliver target");
                    Err(format!("no local actor for target {}", req.target))
                };

                if req.expect_ack {
                    let (ok, error) = match send_result {
                        Ok(()) => (true, String::new()),
                        Err(e) => (false, e),
                    };
                    let _ = reply_tx
                        .send(DeliverReply {
                            frame_id: req.frame_id,
                            ok,
                            error,
                        })
                        .await;
                }
            }
        });

        let stream = ReceiverStream::new(reply_rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}

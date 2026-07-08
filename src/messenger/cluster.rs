//! Dynamic cluster membership: peer join/leave, hash-ring rebalance, shard handoff.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::hash_ring::{HashRing, RingNode};

use super::auth::HmacAuthenticator;
use super::codec::Packet;
use super::wire;
use super::MessengerError;
use super::PeerAddr;
use super::server::{Group, State, StoredMessage};

/// Mutable cluster runtime (interior mutability for hot-path reads).
pub(super) struct ClusterRuntime {
    pub node_id: String,
    pub listen_addr: String,
    pub peer_secret: String,
    pub ring: RwLock<HashRing>,
    pub peer_txs: RwLock<HashMap<String, mpsc::Sender<Packet>>>,
    pub peer_auth: HmacAuthenticator,
}

impl ClusterRuntime {
    pub fn home_of(&self, key: &str) -> String {
        let lookup = key.to_string();
        self.ring
            .read()
            .ok()
            .and_then(|r| r.get_node(&lookup).map(|n| n.id.clone()))
            .unwrap_or_else(|| self.node_id.clone())
    }

    pub fn is_home(&self, key: &str) -> bool {
        self.home_of(key) == self.node_id
    }

    pub fn node_count(&self) -> usize {
        self.ring.read().map(|r| r.node_count()).unwrap_or(1)
    }
}

/// Outbound peer link task (shared with server.rs).
pub(super) async fn peer_link(
    self_node: String,
    peer: PeerAddr,
    token: String,
    mut rx: mpsc::Receiver<Packet>,
) {
    use futures_util::SinkExt;
    use tokio_util::codec::Framed;

    use crate::stream;
    use super::codec::FrameCodec;

    let mut backoff = Duration::from_millis(100);
    loop {
        let socket = match stream::connect(&peer.addr, None).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(peer = %peer.node_id, error = %e, "peer connect failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };
        backoff = Duration::from_millis(100);
        let mut framed = Framed::new(socket, FrameCodec::default());
        if framed
            .send(Packet::PeerHello(super::wire::PeerHello {
                node_id: self_node.clone(),
                auth_token: token.clone(),
            }))
            .await
            .is_err()
        {
            continue;
        }
        tracing::info!(peer = %peer.node_id, "peer link established");
        while let Some(pkt) = rx.recv().await {
            if let Err(e) = framed.send(pkt).await {
                tracing::warn!(peer = %peer.node_id, error = %e, "peer link broken; reconnecting");
                break;
            }
        }
    }
}

/// Register a new peer: update the ring, start an outbound link, gossip, hand
/// off any shards we no longer own.
pub(super) async fn add_peer(
    state: &Arc<State>,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    peer: PeerAddr,
) -> Result<(), MessengerError> {
    let cluster = state.cluster.as_ref().ok_or_else(|| {
        MessengerError::Protocol("add_peer requires bind_cluster".into())
    })?;
    if peer.node_id == cluster.node_id {
        return Err(MessengerError::Protocol("cannot add self as peer".into()));
    }
    if cluster
        .peer_txs
        .read()
        .map_err(|_| MessengerError::Protocol("cluster peer map poisoned".into()))?
        .contains_key(&peer.node_id)
    {
        return Ok(());
    }

    let owned = snapshot_owned_shards(state, cluster);
    {
        let mut ring = cluster
            .ring
            .write()
            .map_err(|_| MessengerError::Protocol("cluster ring poisoned".into()))?;
        if !ring.contains(&peer.node_id) {
            ring.add_node(RingNode::new(peer.node_id.clone(), "peer", 0));
        }
    }
    ensure_outbound_link(cluster, peer_tasks, &peer).await;
    super::server::send_to_all_peers_except(
        state,
        &peer.node_id,
        &Packet::PeerJoin(wire::PeerJoin {
            node_id: peer.node_id.clone(),
            addr: peer.addr.clone(),
        }),
    );
    handoff_lost_shards(state, cluster, &owned).await;
    info!(peer = %peer.node_id, nodes = cluster.node_count(), "peer joined cluster");
    Ok(())
}

/// Remove a peer: gossip leave, hand off shards, drop the ring entry and link.
pub(super) async fn remove_peer(
    state: &Arc<State>,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    node_id: &str,
) -> Result<(), MessengerError> {
    let cluster = state.cluster.as_ref().ok_or_else(|| {
        MessengerError::Protocol("remove_peer requires bind_cluster".into())
    })?;
    if node_id == cluster.node_id {
        return Err(MessengerError::Protocol("cannot remove self".into()));
    }

    super::server::send_to_all_peers_except(
        state,
        node_id,
        &Packet::PeerLeave(wire::PeerLeave { node_id: node_id.to_string() }),
    );
    drop_peer(state, peer_tasks, node_id).await;
    Ok(())
}

/// Inbound gossip: another node joined.
pub(super) async fn handle_peer_join(
    state: &Arc<State>,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    origin: &str,
    join: wire::PeerJoin,
) {
    let Some(cluster) = state.cluster.as_ref() else { return };
    if join.node_id.is_empty() || join.node_id == cluster.node_id {
        return;
    }
    let already = cluster
        .peer_txs
        .read()
        .ok()
        .is_some_and(|t| t.contains_key(&join.node_id))
        && cluster
            .ring
            .read()
            .ok()
            .is_some_and(|r| r.contains(&join.node_id));
    if already {
        return;
    }

    let owned = snapshot_owned_shards(state, cluster);
    {
        let mut ring = cluster.ring.write().expect("cluster ring");
        if !ring.contains(&join.node_id) {
            ring.add_node(RingNode::new(join.node_id.clone(), "peer", 0));
        }
    }
    let peer = PeerAddr { node_id: join.node_id.clone(), addr: join.addr };
    ensure_outbound_link(cluster, peer_tasks, &peer).await;
    super::server::send_to_all_peers_except(
        state,
        origin,
        &Packet::PeerJoin(wire::PeerJoin {
            node_id: peer.node_id.clone(),
            addr: peer.addr.clone(),
        }),
    );
    handoff_lost_shards(state, cluster, &owned).await;
}

/// Inbound gossip: a node left.
pub(super) async fn handle_peer_leave(
    state: &Arc<State>,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    leave: wire::PeerLeave,
) {
    let Some(cluster) = state.cluster.as_ref() else { return };
    if leave.node_id.is_empty() || leave.node_id == cluster.node_id {
        return;
    }
    drop_peer(state, peer_tasks, &leave.node_id).await;
}

/// Drop a peer from the ring and hand off shards (no gossip).
async fn drop_peer(
    state: &Arc<State>,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    node_id: &str,
) {
    let Some(cluster) = state.cluster.as_ref() else { return };
    let owned = snapshot_owned_shards(state, cluster);
    {
        let mut ring = cluster.ring.write().expect("cluster ring");
        ring.remove_node(node_id);
    }
    {
        let mut txs = cluster.peer_txs.write().expect("peer_txs");
        txs.remove(node_id);
    }
    if let Ok(mut tasks) = peer_tasks.lock() {
        if let Some(handle) = tasks.remove(node_id) {
            handle.abort();
        }
    }
    handoff_lost_shards(state, cluster, &owned).await;
    info!(node_id, nodes = cluster.node_count(), "peer removed from cluster");
}

/// Merge an inbox handoff from a former home shard (idempotent via `seen`).
pub(super) async fn merge_handoff_user(state: &Arc<State>, h: wire::PeerHandoffUser) {
    let mut inboxes = state.inboxes.lock().await;
    let inbox = inboxes.entry(h.user_id.clone()).or_default();
    inbox.next_seq = inbox.next_seq.max(h.next_seq);
    for key in h.seen {
        inbox.seen.insert(key);
    }
    for m in h.chats {
        let key = m.message_id.clone();
        if !inbox.seen.insert(key) {
            continue;
        }
        inbox.next_seq = inbox.next_seq.max(m.seq);
        inbox.pending.push_back(StoredMessage {
            seq: m.seq,
            packet: Packet::ChatMessage(m),
            delivered: false,
        });
    }
    for m in h.groups {
        let key = format!("{}:{}", m.message_id, m.to_user);
        if !inbox.seen.insert(key) {
            continue;
        }
        inbox.next_seq = inbox.next_seq.max(m.seq);
        inbox.pending.push_back(StoredMessage {
            seq: m.seq,
            packet: Packet::GroupMessage(m),
            delivered: false,
        });
    }
    let mut pending: Vec<_> = inbox.pending.drain(..).collect();
    pending.sort_by_key(|s| s.seq);
    inbox.pending = pending.into();
    info!(user = %h.user_id, pending = inbox.pending.len(), "inbox handoff merged");
}

/// Merge group state from a former home shard (keep highest version).
pub(super) async fn merge_handoff_group(state: &Arc<State>, h: wire::PeerHandoffGroup) {
    let mut groups = state.groups.lock().await;
    let entry = groups.entry(h.group_id.clone()).or_insert_with(|| Group {
        members: HashSet::new(),
        admins: HashSet::new(),
        version: 0,
    });
    if h.version >= entry.version {
        entry.members = h.members.into_iter().collect();
        entry.admins = h.admins.into_iter().collect();
        entry.version = h.version;
        info!(group = %h.group_id, version = h.version, "group handoff merged");
    }
}

struct OwnedShards {
    users: Vec<String>,
    groups: Vec<String>,
}

/// Users/groups this node currently owns (homeshard + local data).
fn snapshot_owned_shards(state: &Arc<State>, cluster: &ClusterRuntime) -> OwnedShards {
    let users: Vec<String> = state
        .inboxes
        .try_lock()
        .map(|i| {
            i.keys()
                .filter(|u| cluster.is_home(u))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let groups: Vec<String> = state
        .groups
        .try_lock()
        .map(|g| {
            g.keys()
                .filter(|id| cluster.is_home(id))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    OwnedShards { users, groups }
}

async fn handoff_lost_shards(state: &Arc<State>, cluster: &ClusterRuntime, owned: &OwnedShards) {
    for user in &owned.users {
        if cluster.is_home(user) {
            continue;
        }
        let new = cluster.home_of(user);
        if let Some(handoff) = build_user_handoff(state, user).await {
            super::server::send_to_peer(state, &new, Packet::PeerHandoffUser(handoff));
        }
        state.inboxes.lock().await.remove(user);
    }
    for group_id in &owned.groups {
        if cluster.is_home(group_id) {
            continue;
        }
        let new = cluster.home_of(group_id);
        if let Some(handoff) = build_group_handoff(state, group_id).await {
            super::server::send_to_peer(state, &new, Packet::PeerHandoffGroup(handoff));
        }
        state.groups.lock().await.remove(group_id);
    }
}

async fn build_user_handoff(state: &Arc<State>, user: &str) -> Option<wire::PeerHandoffUser> {
    let inboxes = state.inboxes.lock().await;
    let inbox = inboxes.get(user)?;
    let mut chats = Vec::new();
    let mut groups = Vec::new();
    for m in inbox.pending.iter().filter(|m| !m.delivered) {
        match &m.packet {
            Packet::ChatMessage(c) => chats.push(c.clone()),
            Packet::GroupMessage(g) => groups.push(g.clone()),
            _ => {}
        }
    }
    Some(wire::PeerHandoffUser {
        user_id: user.to_string(),
        next_seq: inbox.next_seq,
        seen: inbox.seen.iter().cloned().collect(),
        chats,
        groups,
    })
}

async fn build_group_handoff(state: &Arc<State>, group_id: &str) -> Option<wire::PeerHandoffGroup> {
    let groups = state.groups.lock().await;
    let g = groups.get(group_id)?;
    Some(wire::PeerHandoffGroup {
        group_id: group_id.to_string(),
        members: g.members.iter().cloned().collect(),
        admins: g.admins.iter().cloned().collect(),
        version: g.version,
    })
}

async fn ensure_outbound_link(
    cluster: &ClusterRuntime,
    peer_tasks: &Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    peer: &PeerAddr,
) {
    let (tx, rx) = mpsc::channel::<Packet>(4096);
    {
        let mut txs = cluster.peer_txs.write().expect("peer_txs");
        txs.insert(peer.node_id.clone(), tx);
    }
    let token = cluster.peer_auth.mint_token(&cluster.node_id, "peer");
    let handle = tokio::spawn(peer_link(
        cluster.node_id.clone(),
        peer.clone(),
        token,
        rx,
    ));
    if let Ok(mut tasks) = peer_tasks.lock() {
        if let Some(old) = tasks.insert(peer.node_id.clone(), handle) {
            old.abort();
        }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

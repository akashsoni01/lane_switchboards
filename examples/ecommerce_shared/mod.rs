//! Shared types for [`ecommerce_flash_sale`](../ecommerce_flash_sale.rs):
//! supervised order gateways, mesh inventory/billing actors, autoscaling cluster.

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{Cluster, NodeHandle, RemoteMessage};
use lane_switchboards::prost::Message;
use lane_switchboards::supervisor::{
    ChildRegistry, RestartStrategy, SupervisorConfig, SupervisorHandle,
};
use lane_switchboards::supervise_named_child;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const ORDERS_SERVICE: &str = "orders";
pub const INVENTORY_SERVICE: &str = "inventory";
pub const BILLING_SERVICE: &str = "billing";
pub const ORDER_TARGET: &str = "checkout-gateway";
pub const FLASH_SKU: &str = "flash-deal-42";

pub const ORDERS_INITIAL: usize = 2;
pub const ORDERS_MAX: usize = 8;
pub const AUTOSCALE_REQ_PER_REPLICA: u64 = 8;
pub const CHECKOUTS_PER_WAVE: usize = 16;
pub const MAX_LOAD_WAVES: usize = 12;

// --- Autoscaling (orders gRPC cluster) ---

pub struct AutoscalingCluster<M: RemoteMessage> {
    pub cluster: Cluster<M>,
    handles: Vec<NodeHandle<M>>,
    next_replica: usize,
    dispatches: AtomicU64,
    window_base: AtomicU64,
    pub config: AutoscaleConfig,
}

#[derive(Debug, Clone)]
pub struct AutoscaleConfig {
    pub max_replicas: usize,
    pub requests_per_replica_threshold: u64,
    pub scale_step: usize,
}

impl Default for AutoscaleConfig {
    fn default() -> Self {
        Self {
            max_replicas: ORDERS_MAX,
            requests_per_replica_threshold: AUTOSCALE_REQ_PER_REPLICA,
            scale_step: 1,
        }
    }
}

impl<M: RemoteMessage> AutoscalingCluster<M> {
    pub fn new(config: AutoscaleConfig) -> Self {
        Self {
            cluster: Cluster::new(),
            handles: Vec::new(),
            next_replica: 0,
            dispatches: AtomicU64::new(0),
            window_base: AtomicU64::new(0),
            config,
        }
    }

    pub fn len(&self) -> usize {
        self.cluster.len()
    }

    pub fn join_node(&mut self, handle: NodeHandle<M>, replica_id: usize) {
        self.cluster.join(handle.member.clone());
        self.handles.push(handle);
        self.next_replica = self.next_replica.max(replica_id + 1);
    }

    fn record_dispatches(&self, count: u64) {
        self.dispatches.fetch_add(count, Ordering::Relaxed);
    }

    fn load_per_replica(&self) -> u64 {
        let total = self.dispatches.load(Ordering::Relaxed);
        let base = self.window_base.load(Ordering::Relaxed);
        let delta = total.saturating_sub(base);
        let n = self.cluster.len().max(1) as u64;
        delta / n
    }

    pub async fn maybe_scale_up<F, Fut>(
        &mut self,
        service_label: &str,
        mut launch: F,
    ) -> std::io::Result<usize>
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = std::io::Result<NodeHandle<M>>>,
    {
        let per_replica = self.load_per_replica();
        if self.cluster.len() >= self.config.max_replicas {
            return Ok(0);
        }
        if per_replica < self.config.requests_per_replica_threshold {
            return Ok(0);
        }

        let to_add = self
            .config
            .scale_step
            .min(self.config.max_replicas - self.cluster.len());
        let before = self.cluster.len();

        for _ in 0..to_add {
            let id = self.next_replica;
            let handle = launch(id).await?;
            println!(
                "[autoscale {service_label}] {per_replica} checkouts/replica ≥ {} → replica-{id} @ {} ({} → {})",
                self.config.requests_per_replica_threshold,
                handle.address(),
                self.cluster.len(),
                self.cluster.len() + 1
            );
            self.join_node(handle, id);
        }

        self.window_base
            .store(self.dispatches.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(self.cluster.len() - before)
    }

    pub async fn send_round_robin(&self, msg: M) -> std::io::Result<()> {
        self.record_dispatches(1);
        self.cluster.send_round_robin(msg).await
    }

    pub async fn send_by_key<T: Hash>(&self, key: &T, msg: M) -> std::io::Result<()> {
        self.record_dispatches(1);
        self.cluster.send_by_key(key, msg).await
    }
}

// --- Wire messages (gRPC) ---

#[derive(Clone, PartialEq, Message)]
pub struct OrderCommand {
    #[prost(uint32, tag = "1")]
    pub op: u32,
    #[prost(uint64, tag = "2")]
    pub order_id: u64,
    #[prost(string, tag = "3")]
    pub sku: String,
    #[prost(uint32, tag = "4")]
    pub qty: u32,
    #[prost(uint64, tag = "5")]
    pub amount_cents: u64,
}

impl OrderCommand {
    pub const CHECKOUT: u32 = 1;
    pub const HEALTH: u32 = 2;

    pub fn checkout(order_id: u64, sku: impl Into<String>, qty: u32, amount_cents: u64) -> Self {
        Self {
            op: Self::CHECKOUT,
            order_id,
            sku: sku.into(),
            qty,
            amount_cents,
        }
    }

    pub fn health() -> Self {
        Self {
            op: Self::HEALTH,
            order_id: 0,
            sku: String::new(),
            qty: 0,
            amount_cents: 0,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct InventoryMsg {
    #[prost(string, tag = "1")]
    pub sku: String,
    #[prost(uint32, tag = "2")]
    pub qty: u32,
    #[prost(uint64, tag = "3")]
    pub order_id: u64,
}

impl InventoryMsg {
    pub fn reserve(order_id: u64, sku: impl Into<String>, qty: u32) -> Self {
        Self {
            sku: sku.into(),
            qty,
            order_id,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct BillingMsg {
    #[prost(uint64, tag = "1")]
    pub order_id: u64,
    #[prost(uint64, tag = "2")]
    pub amount_cents: u64,
}

impl BillingMsg {
    pub fn charge(order_id: u64, amount_cents: u64) -> Self {
        Self {
            order_id,
            amount_cents,
        }
    }
}

// --- Supervised order gateway (payment + fraud, OneForOne) ---

enum PaymentMsg {
    Authorize { amount_cents: u64 },
}

enum FraudMsg {
    Screen { order_id: u64 },
}

struct PaymentActor {
    label: String,
}

struct FraudActor {
    label: String,
}

#[async_trait::async_trait]
impl Actor<PaymentMsg> for PaymentActor {
    async fn handle(&mut self, msg: PaymentMsg) -> Result<(), ActorProcessingErr> {
        let PaymentMsg::Authorize { amount_cents } = msg;
        println!(
            "[{}] payment authorized {} cents",
            self.label, amount_cents
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<FraudMsg> for FraudActor {
    async fn handle(&mut self, msg: FraudMsg) -> Result<(), ActorProcessingErr> {
        let FraudMsg::Screen { order_id } = msg;
        println!("[{}] fraud screen passed order {order_id}", self.label);
        Ok(())
    }
}

struct ChildSupervisors {
    _payment: SupervisorHandle<PaymentMsg>,
    _fraud: SupervisorHandle<FraudMsg>,
}

#[derive(Clone)]
pub struct OrderGatewayActor {
    pub label: String,
    payment_registry: Arc<ChildRegistry<PaymentMsg, &'static str>>,
    fraud_registry: Arc<ChildRegistry<FraudMsg, &'static str>>,
    inner: Arc<Mutex<Option<ChildSupervisors>>>,
    checkouts: Arc<AtomicU64>,
}

impl OrderGatewayActor {
    pub fn new_replica(replica: usize) -> Self {
        Self {
            label: format!("orders-gateway-{replica}"),
            payment_registry: Arc::new(ChildRegistry::new()),
            fraud_registry: Arc::new(ChildRegistry::new()),
            inner: Arc::new(Mutex::new(None)),
            checkouts: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn one_for_one_config() -> SupervisorConfig {
    SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    }
}

async fn send_child<M>(
    registry: &ChildRegistry<M, &'static str>,
    name: &'static str,
    msg: M,
) -> Result<(), ActorProcessingErr>
where
    M: Send + Sync + 'static,
{
    let actor = registry
        .get(name)
        .ok_or_else(|| format!("child {name} not running"))?;
    actor.send(msg).await.map_err(Into::into)
}

#[async_trait::async_trait]
impl Actor<OrderCommand> for OrderGatewayActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let label = self.label.clone();
        let pay_reg = self.payment_registry.clone();
        let fraud_reg = self.fraud_registry.clone();

        let payment_label = format!("{label}-payment");
        let fraud_label = format!("{label}-fraud");

        let payment_sup = supervise_named_child!(
            "payment",
            pay_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            PaymentActor {
                label: payment_label.clone()
            }
        )
        .await?;

        let fraud_sup = supervise_named_child!(
            "fraud",
            fraud_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            FraudActor {
                label: fraud_label.clone()
            }
        )
        .await?;

        println!("[{label}] gateway up (supervised payment + fraud, OneForOne)");
        *self.inner.lock().await = Some(ChildSupervisors {
            _payment: payment_sup,
            _fraud: fraud_sup,
        });
        Ok(())
    }

    async fn handle(&mut self, msg: OrderCommand) -> Result<(), ActorProcessingErr> {
        match msg.op {
            OrderCommand::CHECKOUT => {
                let n = self.checkouts.fetch_add(1, Ordering::Relaxed) + 1;
                send_child(
                    &self.payment_registry,
                    "payment",
                    PaymentMsg::Authorize {
                        amount_cents: msg.amount_cents,
                    },
                )
                .await?;
                send_child(
                    &self.fraud_registry,
                    "fraud",
                    FraudMsg::Screen {
                        order_id: msg.order_id,
                    },
                )
                .await?;
                println!(
                    "[{}] checkout #{n} order {} sku={} qty={} (local supervised path ok)",
                    self.label, msg.order_id, msg.sku, msg.qty
                );
                Ok(())
            }
            OrderCommand::HEALTH => {
                println!("[{}] health ok", self.label);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        if let Some(inner) = self.inner.lock().await.take() {
            inner._payment.stop().await;
            inner._fraud.stop().await;
        }
        println!("[{}] gateway stopped", self.label);
        Ok(())
    }
}

// --- Mesh inventory / billing actors ---

pub struct InventoryActor {
    pub instance: String,
}

#[async_trait::async_trait]
impl Actor<InventoryMsg> for InventoryActor {
    async fn handle(&mut self, msg: InventoryMsg) -> Result<(), ActorProcessingErr> {
        println!(
            "[inventory:{}] reserve order {} sku={} qty={}",
            self.instance, msg.order_id, msg.sku, msg.qty
        );
        Ok(())
    }
}

pub struct BillingActor {
    pub instance: String,
}

#[async_trait::async_trait]
impl Actor<BillingMsg> for BillingActor {
    async fn handle(&mut self, msg: BillingMsg) -> Result<(), ActorProcessingErr> {
        println!(
            "[billing:{}] charge order {} amount={}c",
            self.instance, msg.order_id, msg.amount_cents
        );
        Ok(())
    }
}

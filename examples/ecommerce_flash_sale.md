# E-commerce flash sale — mesh, supervision, autoscaling

[`ecommerce_flash_sale.rs`](./ecommerce_flash_sale.rs) models a **limited-stock flash sale** on a small e-commerce platform:

| Layer | Technology | Role |
|-------|------------|------|
| **Orders** | gRPC `Cluster` + **autoscaling** | Checkout gateways; scale out when per-replica load rises |
| **Supervision** | OneForOne per gateway | **Payment** + **fraud** children restart independently |
| **Inventory** | gRPC **service mesh** + **QUORUM** | Reserve stock on 3 replicas (W=2 acks) |
| **Billing** | Mesh hash-ring `invoke` | Charge after reserve |
| **Discovery** | `MeshRegistry` gRPC | Register inventory/billing instances |

```bash
cargo run --example ecommerce_flash_sale
```

Shared actors and autoscale helper: [`ecommerce_shared/mod.rs`](./ecommerce_shared/mod.rs).

---

## Production-shaped deployment

```mermaid
flowchart TB
    subgraph edge ["Storefront / API gateway"]
        API["Checkout API"]
    end

    subgraph orders_tier ["Orders tier — autoscaling gRPC cluster"]
        subgraph pod1 ["Pod orders-replica-0"]
            G1["OrderGatewayActor"]
            S1["Supervisor OneForOne"]
            P1["PaymentActor"]
            F1["FraudActor"]
            G1 --> S1
            S1 --> P1
            S1 --> F1
        end
        subgraph pod2 ["Pod orders-replica-N"]
            G2["OrderGatewayActor"]
            S2["Supervisor"]
            G2 --> S2
        end
        AS["AutoscalingCluster<br/>threshold checkouts/replica"]
    end

    subgraph mesh ["Service mesh — gRPC"]
        REG["MeshRegistry"]
        INV1["inventory inv-0"]
        INV2["inventory inv-1"]
        INV3["inventory inv-2"]
        BILL1["billing bill-0"]
        BILL2["billing bill-1"]
        REG --> INV1
        REG --> INV2
        REG --> INV3
        REG --> BILL1
        REG --> BILL2
    end

    API --> AS
    AS --> pod1
    AS --> pod2
    API -->|"invoke_consistent QUORUM"| INV1
    API --> INV2
    API --> INV3
    API -->|"invoke billing"| BILL1
```

---

## Checkout saga (one sale wave)

```mermaid
sequenceDiagram
    participant Coord as Flash-sale coordinator
    participant Ord as Orders cluster
    participant Pay as Payment child
    participant Inv as Inventory mesh
    participant Bill as Billing mesh

    loop Each checkout in wave
        Coord->>Ord: OrderCommand CHECKOUT round-robin
        Ord->>Pay: Authorize amount
        Ord->>Ord: Fraud screen
        Coord->>Inv: invoke_consistent Reserve QUORUM
        Note over Inv: 3 replicas, W=2 DeliverReply acks
        Coord->>Bill: invoke Charge sticky key
    end

    Coord->>Ord: maybe_scale_up if load/replica high
    Note over Ord: New serve_actor replica joins Cluster ring
```

---

## Autoscaling policy

| Constant | Default | Meaning |
|----------|---------|---------|
| `ORDERS_INITIAL` | 2 | Replicas at boot |
| `ORDERS_MAX` | 8 | Ceiling |
| `AUTOSCALE_REQ_PER_REPLICA` | 8 | Avg checkouts/replica in window → scale out |
| `CHECKOUTS_PER_WAVE` | 16 | Synthetic traffic per wave |

Implementation: [`AutoscalingCluster`](./ecommerce_shared/mod.rs) wraps [`Cluster`](../src/distributed.rs) — same pattern as [`service_complex_cluster.rs`](./service_complex_cluster.rs).

---

## Supervision inside each order gateway

```mermaid
flowchart LR
    subgraph gateway ["OrderGatewayActor"]
        H["handle CHECKOUT"]
    end
    subgraph sup ["Supervisor OneForOne"]
        PAY["payment"]
        FRD["fraud"]
    end
    H --> PAY
    H --> FRD
    PAY -.->|"panic"| PAY2["restarted payment only"]
```

Payment failure does **not** restart fraud (unlike RestForOne). For dependency chains (cart → payment → email), use RestForOne — see [`horizontal_scaling_rest_for_one.rs`](./horizontal_scaling_rest_for_one.rs).

---

## Benchmarks

Micro-benchmarks (localhost, release, Criterion):

```bash
cargo bench --bench wire        # primitives: send, registry list, quorum
cargo bench --bench ecommerce   # full checkout pipeline
```

| Bench | What it measures |
|-------|------------------|
| `wire::remote_actor_ref_send` | Single gRPC deliver on warm stream (~µs) |
| `wire::mesh_registry_list_32` | Control-plane list |
| `wire::invoke_consistent_quorum_rf3` | Inventory-style quorum only |
| `ecommerce::ecommerce_checkout_pipeline` | Order send + QUORUM reserve + billing invoke |

Published numbers (Apple Silicon, release, one run):

| Bench | Median |
|-------|--------|
| `ecommerce_checkout_pipeline` | **~84 µs** |
| `wire::invoke_consistent_quorum_rf3` | **~139 µs** |
| `wire::remote_actor_ref_send` | **~1.8 µs** |

Full table: [README.md](../README.md#benchmarks).

---

## Related examples

| Example | Focus |
|---------|--------|
| [`service_mesh.rs`](./service_mesh.rs) | Mesh only (orders/inventory/billing) |
| [`service_complex_cluster.rs`](./service_complex_cluster.rs) | Autoscale + supervised DAO trees |
| [`consistency.rs`](./consistency.rs) | QUORUM inventory + TLS |
| [`horizontal_scaling.rs`](./horizontal_scaling.rs) | Cluster hash-ring without mesh |

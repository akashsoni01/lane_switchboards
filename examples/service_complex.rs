//! Same DAO layout as [`service`](./service.rs), but **`ServiceASupervisor` and
//! `ServiceBSupervisor` are supervised actors** (each wrapped in `supervise_actor`).
//!
//! Inner DAOs still use one-child supervisors via [`supervise_named_child!`] because each
//! DAO has a distinct message type.
//!
//! Run: `cargo run --example service_complex`
//! Multi-node (10 replicas each): `cargo run --example service_complex_cluster`
//! See: `examples/service_complex.md`

mod service_complex_shared;

use lane_switchboards::actor::ActorRef;
use std::time::Duration;
use service_complex_shared::{
    actor_err, print_gens, service_a_generations, service_b_generations,
    start_supervised_service_a, start_supervised_service_b, ServiceACommand, ServiceBCommand,
    SERVICE_A, SERVICE_B,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== service_complex: supervised ServiceA / ServiceB actors ===\n");

    let (service_a, _sup_a, dao_a_reg, dao_b_reg) =
        start_supervised_service_a().await.map_err(actor_err)?;
    let (service_b, _sup_b, dao_b_reg_b, dao_c_reg) =
        start_supervised_service_b().await.map_err(actor_err)?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("\n--- Initial ping ---");
    ping_service_a(&service_a).await?;
    ping_service_b(&service_b).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n--- Crash DaoB under ServiceASupervisor only ---");
    let before = service_a_generations(&dao_a_reg, &dao_b_reg).await;
    service_a
        .send(ServiceACommand::fail_dao_b())
        .await
        .map_err(actor_err)?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = service_a_generations(&dao_a_reg, &dao_b_reg).await;
    print_gens(
        SERVICE_A,
        "[generations]",
        &["dao-a", "dao-b"],
        &before,
        &after,
    );

    let b_before = service_b_generations(&dao_b_reg_b, &dao_c_reg).await;
    ping_service_b(&service_b).await?;
    let b_after = service_b_generations(&dao_b_reg_b, &dao_c_reg).await;
    print_gens(
        SERVICE_B,
        "[ServiceBSupervisor unchanged]",
        &["dao-b", "dao-c"],
        &b_before,
        &b_after,
    );

    println!("\n--- Crash DaoC under ServiceBSupervisor only ---");
    let before = service_b_generations(&dao_b_reg_b, &dao_c_reg).await;
    service_b
        .send(ServiceBCommand::fail_dao_c())
        .await
        .map_err(actor_err)?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = service_b_generations(&dao_b_reg_b, &dao_c_reg).await;
    print_gens(
        SERVICE_B,
        "[generations]",
        &["dao-b", "dao-c"],
        &before,
        &after,
    );

    let a_before = service_a_generations(&dao_a_reg, &dao_b_reg).await;
    ping_service_a(&service_a).await?;
    let a_after = service_a_generations(&dao_a_reg, &dao_b_reg).await;
    print_gens(
        SERVICE_A,
        "[ServiceASupervisor unchanged]",
        &["dao-a", "dao-b"],
        &a_before,
        &a_after,
    );

    println!("\n--- Done: DAO crash = inner sup; service actor has outer sup ---");
    Ok(())
}

async fn ping_service_a(service: &ActorRef<ServiceACommand>) -> anyhow::Result<()> {
    service
        .send(ServiceACommand::ping_all())
        .await
        .map_err(actor_err)
}

async fn ping_service_b(service: &ActorRef<ServiceBCommand>) -> anyhow::Result<()> {
    service
        .send(ServiceBCommand::ping_all())
        .await
        .map_err(actor_err)
}

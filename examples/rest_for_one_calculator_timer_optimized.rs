//! RestForOne calculator + timer (optimized example).
//!
//! Uses library macros to reduce boilerplate:
//! - `registry_child_spec!` for named child specs
//! - `actor_ask!` for oneshot request/reply
//!
//! Run: `cargo run --example rest_for_one_calculator_timer_optimized`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    ChildRegistry, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use lane_switchboards::{actor_ask, registry_child_spec};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ChildName {
    Calculator,
    Timer,
}

// impl AsRef<str> for ChildName {
//     fn as_ref(&self) -> &str {
//         match self {
//             ChildName::Calculator => "calculator",
//             ChildName::Timer => "timer",
//         }
//     }
// }

// impl From<ChildName> for String {
//     fn from(value: ChildName) -> Self {
//         value.as_ref().to_string()
//     }
// }

enum AppMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    LastResult(oneshot::Sender<Option<f64>>),
    TimerStart(ActorRef<AppMsg>),
    TimerTick,
}

#[derive(Clone)]
struct Calculator {
    last_result: Option<f64>,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for Calculator {
    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::Add(a, b, reply) => {
                let value = a + b;
                self.last_result = Some(value);
                let _ = reply.send(Ok(value));
            }
            AppMsg::Div(a, b, reply) => {
                if b == 0.0 {
                    panic!("division by zero");
                }
                let value = a / b;
                self.last_result = Some(value);
                let _ = reply.send(Ok(value));
            }
            AppMsg::LastResult(reply) => {
                let _ = reply.send(self.last_result);
            }
            AppMsg::TimerStart(_) | AppMsg::TimerTick => {}
        }
        Ok(())
    }
}

struct ResultTimer {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    self_ref: Option<ActorRef<AppMsg>>,
    interval: Duration,
    running: bool,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for ResultTimer {
    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::TimerStart(self_ref) => {
                self.self_ref = Some(self_ref);
                self.running = true;
                self.schedule_next();
            }
            AppMsg::TimerTick if self.running => {
                if let Some(calc) = self.registry.get(&ChildName::Calculator).await {
                    match actor_ask!(calc, |reply| AppMsg::LastResult(reply)) {
                        Ok(Some(v)) => println!("[timer] last_result = {v}"),
                        Ok(None) => println!("[timer] last_result = (none)"),
                        Err(_) => println!("[timer] calculator unavailable"),
                    }
                }
                self.schedule_next();
            }
            _ => {}
        }
        Ok(())
    }
}

impl ResultTimer {
    fn schedule_next(&self) {
        let Some(self_ref) = self.self_ref.clone() else {
            return;
        };
        let delay = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = self_ref.send(AppMsg::TimerTick).await;
        });
    }
}

struct App {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    _supervisor: SupervisorHandle<AppMsg>,
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

impl App {
    fn rest_for_one_config(cfg: SupervisorConfig) -> SupervisorConfig {
        SupervisorConfig {
            strategy: RestartStrategy::RestForOne,
            ..cfg
        }
    }

    fn child_specs(
        interval: Duration,
        registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    ) -> Vec<Box<dyn lane_switchboards::supervisor::ChildSpec<AppMsg>>> {
        vec![
            registry_child_spec!(0, ChildName::Calculator, registry, Calculator { last_result: None }),
            registry_child_spec!(
                1,
                ChildName::Timer,
                registry,
                ResultTimer {
                    registry: registry.clone(),
                    self_ref: None,
                    interval,
                    running: false,
                }
            ),
        ]
    }

    async fn start(interval: Duration, cfg: SupervisorConfig) -> Result<Self, ActorProcessingErr> {
        let registry = Arc::new(ChildRegistry::<AppMsg, ChildName>::new());
        let handle = Supervisor::new(
            Self::rest_for_one_config(cfg),
            Self::child_specs(interval, registry.clone()),
        )
        .start_settled(Duration::from_millis(50))
        .await?;
        Ok(Self {
            registry,
            _supervisor: handle,
        })
    }

    async fn start_timer(&self) -> anyhow::Result<()> {
        let timer = self
            .registry
            .get(&ChildName::Timer)
            .await
            .ok_or_else(|| anyhow::anyhow!("timer not running"))?;
        timer
            .send(AppMsg::TimerStart(timer.clone()))
            .await
            .map_err(actor_err)?;
        Ok(())
    }

    async fn add(&self, a: f64, b: f64) -> anyhow::Result<f64> {
        let calc = self
            .registry
            .get(&ChildName::Calculator)
            .await
            .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
        actor_ask!(calc, |reply| AppMsg::Add(a, b, reply))
            .map_err(actor_err)?
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn div(&self, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
        let calc = self
            .registry
            .get(&ChildName::Calculator)
            .await
            .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
        match actor_ask!(calc, |reply| AppMsg::Div(a, b, reply)) {
            Ok(v) => Ok(v),
            Err(_) => Err(anyhow::anyhow!(
                "calculator crashed (RestForOne restarts calculator + timer)"
            )),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let interval = Duration::from_millis(600);

    let app = App::start(
        interval,
        SupervisorConfig {
            max_restarts: 10,
            within_secs: 60,
            intensity_action: IntensityAction::ShutdownSupervisor,
            ..Default::default()
        },
    )
    .await
    .map_err(actor_err)?;

    println!("=== Optimized RestForOne demo ===");
    app.start_timer().await?;
    println!("[calc] add 10 + 4 = {}", app.add(10.0, 4.0).await?);
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("--- div by zero; RestForOne should restart both ---");
    let _ = app.div(10.0, 0.0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("[calc] add 1 + 1 = {}", app.add(1.0, 1.0).await?);

    println!("\n=== Intensity breach demo ===");
    let app = App::start(
        interval,
        SupervisorConfig {
            max_restarts: 2,
            within_secs: 10,
            intensity_action: IntensityAction::ShutdownSupervisor,
            ..Default::default()
        },
    )
    .await
    .map_err(actor_err)?;
    app.start_timer().await?;
    let _ = app.add(2.0, 2.0).await?;
    for i in 1..=4 {
        println!("--- failure {i} ---");
        let _ = app.div(1.0, 0.0).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    match app.add(99.0, 1.0).await {
        Ok(v) => println!("[calc] unexpected success: {v}"),
        Err(e) => println!("[calc] supervisor dead — add failed: {e}"),
    }
    Ok(())
}

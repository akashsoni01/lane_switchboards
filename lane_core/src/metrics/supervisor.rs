//! Supervisor restart and intensity metrics (lazy-registered).

use crate::supervisor::{IntensityAction, RestartStrategy};
use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Registry};
use std::sync::RwLock;

use super::common::{metrics_try, register_counter_vec, register_gauge_vec, global_node};

pub(crate) struct SupervisorMetricsRegistry {
    restarts: IntCounterVec,
    intensity_exceeded: IntCounterVec,
    children_alive: prometheus::GaugeVec,
    intensity_remaining: prometheus::GaugeVec,
    child_generation: prometheus::GaugeVec,
}

impl SupervisorMetricsRegistry {
    pub(crate) fn register(registry: &Registry) -> Self {
        let node_label = &["node"];
        Self {
            restarts: register_counter_vec(
                registry,
                "lane_supervisor_restarts_total",
                "Supervised child restarts",
                &["child", "strategy", "node"],
            ),
            intensity_exceeded: register_counter_vec(
                registry,
                "lane_supervisor_intensity_exceeded_total",
                "Restart intensity limit breached",
                &["action", "node"],
            ),
            children_alive: register_gauge_vec(
                registry,
                "lane_supervisor_children_alive",
                "Currently live children under the supervisor",
                node_label,
            ),
            intensity_remaining: register_gauge_vec(
                registry,
                "lane_supervisor_restart_intensity_remaining",
                "Restart budget remaining in the current window",
                node_label,
            ),
            child_generation: register_gauge_vec(
                registry,
                "lane_supervisor_child_generation",
                "Child restart generation from ChildRegistry",
                &["child", "node"],
            ),
        }
    }

    pub(crate) fn record_restart(&self, child: &str, strategy: RestartStrategy) {
        metrics_try("record_supervisor_restart", || {
            let node = global_node();
            let strategy = super::restart_strategy_label(strategy);
            if let Ok(counter) = self
                .restarts
                .get_metric_with_label_values(&[child, strategy, &node])
            {
                counter.inc();
            }
        });
    }

    pub(crate) fn record_intensity_exceeded(&self, action: IntensityAction) {
        metrics_try("record_intensity_exceeded", || {
            let node = global_node();
            let action = intensity_action_label(action);
            if let Ok(counter) = self
                .intensity_exceeded
                .get_metric_with_label_values(&[action, &node])
            {
                counter.inc();
            }
        });
    }

    pub(crate) fn set_child_generation(&self, child: &str, generation: u64) {
        metrics_try("set_child_generation", || {
            let node = global_node();
            if let Ok(gauge) = self
                .child_generation
                .get_metric_with_label_values(&[child, &node])
            {
                gauge.set(generation as f64);
            }
        });
    }

    pub(crate) fn sync_scrape_gauges(&self, state: &SupervisorScrapeState) {
        metrics_try("sync_supervisor_prometheus_gauges", || {
            let node = global_node();
            if let Ok(gauge) = self.children_alive.get_metric_with_label_values(&[&node]) {
                gauge.set(state.children_alive as f64);
            }
            if let Ok(gauge) = self.intensity_remaining.get_metric_with_label_values(&[&node]) {
                gauge.set(
                    state
                        .max_restarts
                        .saturating_sub(state.restarts_in_window) as f64,
                );
            }
        });
    }
}

#[derive(Default, Clone)]
pub(crate) struct SupervisorScrapeState {
    pub restarts_in_window: usize,
    pub max_restarts: usize,
    pub children_alive: usize,
}

static SUPERVISOR_SCRAPE: Lazy<RwLock<SupervisorScrapeState>> =
    Lazy::new(|| RwLock::new(SupervisorScrapeState::default()));

pub fn sync_supervisor_scrape(
    restarts_in_window: usize,
    max_restarts: usize,
    children_alive: usize,
) {
    if let Ok(mut state) = SUPERVISOR_SCRAPE.write() {
        state.restarts_in_window = restarts_in_window;
        state.max_restarts = max_restarts;
        state.children_alive = children_alive;
    }
}

pub(crate) fn supervisor_scrape_state() -> SupervisorScrapeState {
    SUPERVISOR_SCRAPE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

fn intensity_action_label(action: IntensityAction) -> &'static str {
    match action {
        IntensityAction::ShutdownSupervisor => "shutdown_supervisor",
        IntensityAction::AbandonChild => "abandon_child",
    }
}

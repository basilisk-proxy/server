use crate::models::InstanceStatus;
use crate::service_bus::contracts::ServiceBusEventEnvelope;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use std::time::Duration;

/// Aggregates runtime metrics collected by Basilisk.
pub struct RuntimeTelemetry {
    proxy_latency: DashMap<String, LatencySummary>,
    service_distributions: DashMap<String, DistributionSummary>,
    static_forward_routes: DashMap<String, StaticForwardSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub total_micros: u128,
    pub avg_micros: u128,
    pub max_micros: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct DistributionSummary {
    pub service_id: String,
    pub metric: String,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub last: f64,
    pub last_seen_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeTelemetrySnapshot {
    pub proxy_latency: Vec<(String, LatencySummary)>,
    pub service_distributions: Vec<DistributionSummary>,
    pub static_forward_routes: Vec<StaticForwardSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaticForwardSummary {
    pub route_id: String,
    pub configured_upstreams: usize,
    pub reachable_upstreams: usize,
    pub status: InstanceStatus,
    pub selected_target: Option<String>,
    pub last_seen_utc: DateTime<Utc>,
}

impl Default for RuntimeTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeTelemetry {
    pub fn new() -> Self {
        Self {
            proxy_latency: DashMap::new(),
            service_distributions: DashMap::new(),
            static_forward_routes: DashMap::new(),
        }
    }

    /// Records latency for a named proxy/runtime phase.
    pub fn record_proxy_latency(&self, phase: &str, elapsed: Duration) {
        let micros = elapsed.as_micros();
        let mut entry = self
            .proxy_latency
            .entry(phase.to_string())
            .or_insert_with(|| LatencySummary {
                count: 0,
                total_micros: 0,
                avg_micros: 0,
                max_micros: 0,
            });

        entry.count += 1;
        entry.total_micros += micros;
        entry.avg_micros = entry.total_micros / entry.count as u128;
        entry.max_micros = entry.max_micros.max(micros);
    }

    /// Ingests one `basilisk.metrics.distribution` event from a connected service.
    pub fn ingest_distribution_event(&self, event: &ServiceBusEventEnvelope) {
        let metric_name = event
            .payload
            .get("metric")
            .or_else(|| event.payload.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("unnamed")
            .to_string();

        let value = event
            .payload
            .get("value")
            .and_then(json_to_f64)
            .unwrap_or(1.0);

        let key = format!("{}::{}", event.service_id, metric_name);
        let mut entry =
            self.service_distributions
                .entry(key)
                .or_insert_with(|| DistributionSummary {
                    service_id: event.service_id.clone(),
                    metric: metric_name,
                    count: 0,
                    sum: 0.0,
                    min: value,
                    max: value,
                    last: value,
                    last_seen_utc: Utc::now(),
                });

        entry.count += 1;
        entry.sum += value;
        entry.min = entry.min.min(value);
        entry.max = entry.max.max(value);
        entry.last = value;
        entry.last_seen_utc = Utc::now();
    }

    /// Records the current health of a statically configured forwarding route.
    pub fn record_static_forward_health(
        &self,
        route_id: &str,
        configured_upstreams: usize,
        reachable_upstreams: usize,
        selected_target: Option<&str>,
    ) {
        let status = if reachable_upstreams == 0 {
            InstanceStatus::Down
        } else if reachable_upstreams < configured_upstreams {
            InstanceStatus::Degraded
        } else {
            InstanceStatus::Up
        };

        self.static_forward_routes.insert(
            route_id.to_string(),
            StaticForwardSummary {
                route_id: route_id.to_string(),
                configured_upstreams,
                reachable_upstreams,
                status,
                selected_target: selected_target.map(ToString::to_string),
                last_seen_utc: Utc::now(),
            },
        );
    }

    pub fn snapshot(&self) -> RuntimeTelemetrySnapshot {
        RuntimeTelemetrySnapshot {
            proxy_latency: self
                .proxy_latency
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            service_distributions: self
                .service_distributions
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            static_forward_routes: self
                .static_forward_routes
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
        }
    }
}

fn json_to_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .or_else(|| value.as_u64().map(|v| v as f64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_bus::contracts::ServiceBusEventEnvelope;
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn records_proxy_latency_summary() {
        let telemetry = RuntimeTelemetry::new();
        telemetry.record_proxy_latency("proxy.middleware", Duration::from_micros(100));
        telemetry.record_proxy_latency("proxy.middleware", Duration::from_micros(300));

        let snapshot = telemetry.snapshot();
        let (_, stats) = snapshot
            .proxy_latency
            .into_iter()
            .find(|(name, _)| name == "proxy.middleware")
            .expect("proxy.middleware summary should exist");

        assert_eq!(stats.count, 2);
        assert_eq!(stats.total_micros, 400);
        assert_eq!(stats.avg_micros, 200);
        assert_eq!(stats.max_micros, 300);
    }

    #[test]
    fn ingests_distribution_event() {
        let telemetry = RuntimeTelemetry::new();

        let mut payload = HashMap::new();
        payload.insert("metric".to_string(), serde_json::json!("p95"));
        payload.insert("value".to_string(), serde_json::json!(12.5));

        telemetry.ingest_distribution_event(&ServiceBusEventEnvelope {
            event_id: "evt-1".to_string(),
            emitted_at_utc: Utc::now(),
            service_id: "orders".to_string(),
            instance_id: "orders-1".to_string(),
            topic: "basilisk.metrics.distribution".to_string(),
            message_type: "distribution".to_string(),
            correlation_id: 1,
            causation_id: None,
            payload,
        });

        let snapshot = telemetry.snapshot();
        let metric = snapshot
            .service_distributions
            .into_iter()
            .find(|m| m.service_id == "orders" && m.metric == "p95")
            .expect("distribution summary should exist");

        assert_eq!(metric.count, 1);
        assert_eq!(metric.sum, 12.5);
        assert_eq!(metric.min, 12.5);
        assert_eq!(metric.max, 12.5);
        assert_eq!(metric.last, 12.5);
    }

    #[test]
    fn records_static_forward_health_summary() {
        let telemetry = RuntimeTelemetry::new();

        telemetry.record_static_forward_health("route-1", 2, 1, Some("http://upstream"));

        let snapshot = telemetry.snapshot();
        let route = snapshot
            .static_forward_routes
            .into_iter()
            .find(|route| route.route_id == "route-1")
            .expect("static forward summary should exist");

        assert_eq!(route.configured_upstreams, 2);
        assert_eq!(route.reachable_upstreams, 1);
        assert_eq!(route.status, InstanceStatus::Degraded);
        assert_eq!(route.selected_target.as_deref(), Some("http://upstream"));
    }
}

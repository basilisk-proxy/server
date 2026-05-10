use crate::service_bus::contracts::ServiceBusEventEnvelope;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use std::time::Duration;

/// Aggregates runtime metrics collected by Basilisk.
pub struct RuntimeTelemetry {
    proxy_latency: DashMap<String, LatencySummary>,
    service_distributions: DashMap<String, DistributionSummary>,
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
}

impl RuntimeTelemetry {
    pub fn new() -> Self {
        Self {
            proxy_latency: DashMap::new(),
            service_distributions: DashMap::new(),
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
}

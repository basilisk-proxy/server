use basilisk::models::{AuthInfo, InstanceInfo, InstanceStatus, RegistrationRequest};
use basilisk::registry::ServiceRegistry;

fn registration_request(
    service_id: &str,
    instance_id: &str,
    path_prefixes: Vec<&str>,
) -> RegistrationRequest {
    RegistrationRequest {
        service_id: service_id.to_string(),
        fingerprint: "fp-1".to_string(),
        path_prefixes: path_prefixes.into_iter().map(|v| v.to_string()).collect(),
        instance: InstanceInfo {
            instance_id: instance_id.to_string(),
            scheme: "http".to_string(),
            host: "localhost".to_string(),
            port: 18080,
            weight: 1,
        },
        auth: AuthInfo {
            r#type: "token".to_string(),
            token: "token".to_string(),
        },
    }
}

#[tokio::test]
async fn register_and_deregister_work_for_public_registry_api() {
    let registry = ServiceRegistry::new();
    let request = registration_request("orders", "orders-1", vec!["/api/orders"]);

    let result = registry.register(request.clone()).await;
    assert!(result.success);

    let service = registry
        .get_service("orders")
        .expect("orders service should exist after registration");
    let instance = service
        .instances
        .get("orders-1")
        .expect("orders-1 instance should exist");
    assert_eq!(instance.status, InstanceStatus::Down);

    let token = result.token.expect("registration must return token");
    assert!(registry.validate_instance_token("orders", "orders-1", &token));
    assert!(registry.validate_any_instance_token("orders", &token));

    assert_eq!(
        registry
            .resolve_service_by_path("/api/orders/123")
            .as_deref(),
        Some("orders")
    );

    assert!(registry.deregister("orders", "orders-1").await);
}

#[tokio::test]
async fn route_collision_and_longest_prefix_resolution_are_enforced() {
    let registry = ServiceRegistry::new();

    let first = registration_request("svc-a", "a-1", vec!["/api"]);
    let first_result = registry.register(first).await;
    assert!(first_result.success);

    let colliding = registration_request("svc-b", "b-1", vec!["/api"]);
    let colliding_result = registry.register(colliding).await;
    assert!(!colliding_result.success);
    assert_eq!(
        colliding_result.error_code.as_deref(),
        Some("ROUTE_COLLISION")
    );

    registry.bind_path_prefix("/api/orders".to_string(), "svc-orders".to_string());
    assert_eq!(
        registry
            .resolve_service_by_path("/api/orders/42")
            .as_deref(),
        Some("svc-orders")
    );
}

#[tokio::test]
async fn stale_down_instances_are_removed() {
    let registry = ServiceRegistry::new();
    let request = registration_request("billing", "bill-1", vec!["/api/billing"]);
    let result = registry.register(request).await;
    assert!(result.success);

    registry
        .update_instance_status("billing", "bill-1", InstanceStatus::Down)
        .await;
    registry.remove_stale_instances(std::time::Duration::ZERO);

    let service = registry
        .get_service("billing")
        .expect("service should still exist");
    assert!(service.instances.is_empty());
}

#[tokio::test]
async fn metrics_heartbeat_drives_up_and_stale_down_transitions() {
    let registry = ServiceRegistry::new();
    let request = registration_request("inventory", "inv-1", vec!["/api/inventory"]);
    let result = registry.register(request).await;
    assert!(result.success);

    registry.record_metrics_heartbeat("inventory", "inv-1");
    registry.evaluate_metrics_health(std::time::Duration::from_secs(5));

    let service = registry
        .get_service("inventory")
        .expect("inventory service should exist");
    let instance = service.instances.get("inv-1").expect("inv-1 should exist");
    assert_eq!(instance.status, InstanceStatus::Up);

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    registry.evaluate_metrics_health(std::time::Duration::from_millis(1));

    let service = registry
        .get_service("inventory")
        .expect("inventory service should exist");
    let instance = service.instances.get("inv-1").expect("inv-1 should exist");
    assert_eq!(instance.status, InstanceStatus::Down);
}

#[tokio::test]
async fn metrics_heartbeat_irregularity_marks_instance_degraded() {
    let registry = ServiceRegistry::new();
    let request = registration_request("search", "search-1", vec!["/api/search"]);
    let result = registry.register(request).await;
    assert!(result.success);

    // Establish a stable baseline cadence.
    for _ in 0..5 {
        registry.record_metrics_heartbeat("search", "search-1");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    registry.evaluate_metrics_health(std::time::Duration::from_secs(5));

    // Introduce irregular cadence to simulate heartbeat instability.
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    registry.record_metrics_heartbeat("search", "search-1");
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    registry.record_metrics_heartbeat("search", "search-1");

    registry.evaluate_metrics_health(std::time::Duration::from_secs(5));

    let service = registry
        .get_service("search")
        .expect("search service should exist");
    let instance = service
        .instances
        .get("search-1")
        .expect("search-1 should exist");
    assert_eq!(instance.status, InstanceStatus::Degraded);
}

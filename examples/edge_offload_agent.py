def handler(event):
    try:
        # Serverledge passes parameters directly as dictionary keys
        data = event if isinstance(event, dict) else {}
        
        cpu_load = float(data.get("cpu_load", 0.75))
        queue_length = int(data.get("queue_length", 12))
        available_memory = float(data.get("available_memory", 424.0))
    except Exception:
        cpu_load = 0.75
        queue_length = 12
        available_memory = 424.0

    # AI Agent Multi-Metric Load Scoring Model
    load_score = (cpu_load * 0.5) + (min(queue_length, 50) / 50 * 0.3) + (max(0, (1024 - available_memory) / 1024) * 0.2)
    
    if load_score > 0.85:
        decision = "OFFLOAD_VERTICAL"
        target_node = "cloud_datacenter_primary"
        reasoning = "Critical congestion threshold reached; routing task upstream to cloud backend."
    elif load_score > 0.65:
        decision = "OFFLOAD_HORIZONTAL"
        target_node = "peer_edge_node_rome_secondary"
        reasoning = "Local node approaching capacity; offloading horizontally via etcd cluster coordination."
    else:
        decision = "PROCESS_LOCAL"
        target_node = "current_edge_node_rome"
        reasoning = "Node operating within stable capacity; executing locally in warm container pool."
        
    return {
        "ai_agent_status": "active",
        "calculated_load_score": round(load_score, 3),
        "decision": decision,
        "target_node": target_node,
        "reasoning": reasoning,
        "telemetry_evaluated": {
            "cpu_load": cpu_load,
            "queue_length": queue_length,
            "available_memory_mb": available_memory
        }
    }
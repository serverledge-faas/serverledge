import time
import csv
import os
from locust import HttpUser, task, between, events, constant

CSV_FILE = "experiment_results.csv"

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "function", "response_time_s", "node_arch", "status_code", "policy", "locust_response_time"])

@events.request.add_listener
def on_request(request_type, name, response_time, response_length, response, exception, context, **kwargs):
    policy = os.environ.get("LB_POLICY", "unknown")

    # GESTIONE DEI FALLIMENTI
    if exception:
        print(f"Request failed: {exception}")
        with open(CSV_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            # Scriviamo il fallimento nel CSV. Usiamo il nome dell'eccezione come status code.
            writer.writerow([
                time.time(),
                name,
                "unknown", # Nessun tempo di risposta dal server
                "unknown", # Nessuna architettura
                f"FAILED: {type(exception).__name__}",
                policy,
                response_time or 0
            ])
        return

    node_arch = response.headers.get("Serverledge-Node-Arch", "unknown")
    serverledge_response_time = "unknown"

    try:
        data = response.json()
        if "ResponseTime" in data:
            serverledge_response_time = data["ResponseTime"]
    except Exception:
        pass

    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            time.time(),
            name,
            serverledge_response_time,
            node_arch,
            response.status_code,
            policy,
            response_time
        ])

# --- CLASSI UTENTE ---

class AmdUser(HttpUser):
    wait_time = constant(0.0)
    weight = 1 # Peso relativo

    @task
    def invoke_amd_faster(self):
        self.client.post("/invoke/amd_faster", json={"params": {}}, name="amd_faster", timeout=10)

class ArmUser(HttpUser):
    wait_time = constant(0.0)
    weight = 1 # Peso relativo

    @task
    def invoke_arm_faster(self):
        self.client.post("/invoke/arm_faster", json={"params": {}}, name="arm_faster", timeout=10)

class ThirdFunctionUser(HttpUser):
    wait_time = constant(20.0)
    weight = 1

    def on_start(self):
        time.sleep(30)

    @task
    def invoke_third(self):
        self.client.post("/invoke/amd_only", json={"params": {"duration": 30}}, name="amd_only", timeout=50)
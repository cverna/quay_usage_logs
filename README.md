## Running with Podman (Linux/macOS)

You can run these scripts inside a container using Podman. This ensures a consistent environment and bundles dependencies.

### 1. Prerequisites for Podman Usage

* Podman installed on your system.
* The scripts `get_quay_logs.py` and `compile_log_stats.py` in your project directory.
* The `Dockerfile` (as provided above) in the same directory.

### 2. Build the Podman Image

If you haven't already, build the container image from the directory containing the `Dockerfile` and Python scripts:

```bash
podman build -t quay-usage-logs .
```

### 3. Export the QUAY API token

```bash
export QUAY_API_TOKEN='your_actual_oauth_token_here'
```

### 4. Get the usage logs

```bash
podman run --rm -e QUAY_API_TOKEN -v ./:/app/:Z quay-usage-logs:latest python get_quay_logs.py
```

### 5. Analyze the usage logs

```bash
podman run --rm -e QUAY_API_TOKEN -v ./:/app/:Z quay-usage-logs:latest python compile_log_stats.py quay_fedora_fedora-bootc_logs_last_30d.json 
```
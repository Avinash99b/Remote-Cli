# Deployment Guide — Remote-Cli Fleet Harness

This document provides complete instructions for deploying and operating **Remote-Cli** across local development environments, public tunnels, cloud GPU providers, and remote execution kernels (such as Kaggle and Google Colab).

---

## 1. Architecture & Component Mapping

Remote-Cli consists of three primary components. You can run them directly with Python 3.9+ or use the standalone compiled binaries:

| Component | Source File | Compiled Binary | Default Role & Network Flow |
|---|---|---|---|
| **Central Proxy** | `server.py` | `remote-cli-server` | Listens on port `8765`. Exposes `/notebook` and `/manager` WebSocket paths. Must have a publicly reachable address or tunnel. |
| **Fleet Manager CLI** | `manager.py` | `remote-cli` | Connects outbound to `wss://<PROXY>/manager`. Used by developers and AI agents to execute commands. |
| **Remote Runner** | `client.py` | `client` | Runs inside target environments (Kaggle notebook, Colab, cloud VM). Connects outbound to `wss://<PROXY>/notebook`. |

Because the runner initiates an **outbound connection** to the proxy, workers do **not** require public IPs, port forwarding, or inbound firewall permissions.

---

## 2. Setting Up the Central Proxy

### Option A: Local / Intranet Deployment
If your workers can directly reach your machine (e.g., inside the same LAN or VPN):

```bash
# Using Python
python3 server.py --host 0.0.0.0 --port 8765 --token "your-secure-secret-token"

# Or using compiled binary
./remote-cli-server --host 0.0.0.0 --port 8765 --token "your-secure-secret-token"
```

### Option B: Exposing via Public Tunnel (Cloudflare, Ngrok, Linkpulse)
When controlling remote nodes across the internet (such as Kaggle notebooks), the proxy must have a public HTTPS/WSS endpoint.

#### 1. Using Cloudflare Tunnels (Free & Recommended)
```bash
# Install and run cloudflared
cloudflared tunnel --url http://localhost:8765
```
Cloudflare will output a public URL like `https://random-subdomain.trycloudflare.com`.
The WebSocket URLs will be:
- Runner: `wss://random-subdomain.trycloudflare.com/notebook`
- Manager: `wss://random-subdomain.trycloudflare.com/manager`

#### 2. Using Ngrok
```bash
ngrok http 8765
```
Ngrok provides a URL like `https://xyz.ngrok-free.app`.
The WebSocket URLs will be:
- Runner: `wss://xyz.ngrok-free.app/notebook`
- Manager: `wss://xyz.ngrok-free.app/manager`

---

## 3. Running the Proxy as a Linux Systemd Service

To keep the proxy running 24/7 on a cloud server:

Create `/etc/systemd/system/remote-cli-server.service`:

```ini
[Unit]
Description=Remote-Cli Central Proxy Server
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/remote-cli
ExecStart=/opt/remote-cli/.venv/bin/python server.py --host 0.0.0.0 --port 8765 --token "your-secure-secret-token" --log-file /var/log/remote-cli-server.log
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now remote-cli-server
sudo journalctl -u remote-cli-server -f
```

---

## 4. Deploying the Runner (Client)

### 4.1. Kaggle Notebook Deployment

Kaggle kernels are ephemeral, headless containers. Remote-Cli provides automatic reconnection and identity persistence across notebook execution cells.

#### Method 1: Background Python Thread (Interactive Notebooks)

Insert this code into the top code cell of your Kaggle notebook:

```python
# Cell 1: Bootstrap Remote-Cli and start runner
!pip install -q websockets
!git clone https://github.com/avinash/Remote-Cli.git /kaggle/working/remote-cli 2>/dev/null || (cd /kaggle/working/remote-cli && git pull)

import sys
sys.path.insert(0, "/kaggle/working/remote-cli")

from client import start_in_background

# Start daemon thread that runs silently in the background
thread = start_in_background(
    proxy_url="wss://your-tunnel-domain.com/notebook",
    token="your-secure-secret-token",
    heartbeat_interval=10.0,
    log_file="/kaggle/working/runner.log"
)
print("Remote-Cli runner successfully started in background!")
```

#### Method 2: Detached Process (Headless Notebooks / Scripts)

In a bash cell or script:

```bash
!pip install -q websockets
!git clone https://github.com/avinash/Remote-Cli.git /kaggle/working/remote-cli 2>/dev/null || (cd /kaggle/working/remote-cli && git pull)
!nohup python3 /kaggle/working/remote-cli/client.py \
    --proxy "wss://your-tunnel-domain.com/notebook" \
    --token "your-secure-secret-token" \
    --identity-file "/kaggle/working/.notebook_identity.json" \
    --log-file "/kaggle/working/runner.log" \
    > /dev/null 2>&1 &
```

#### Node Identity Persistence
When the runner first connects, the proxy assigns a persistent ID (e.g. `nb-3f9a12bc4e`) and secret token. These are stored locally in `.notebook_identity.json`:
```json
{
  "notebook_id": "nb-3f9a12bc4e",
  "secret": "9d8e7c6b5a4f1234"
}
```
If network blips occur or if you run the cell again, the runner re-authenticates using this file and retains its original notebook ID without dropping queued commands!

---

### 4.2. Google Colab Deployment

In Google Colab, you can launch the runner in a background cell:

```python
!pip install -q websockets
!git clone https://github.com/avinash/Remote-Cli.git /content/remote-cli 2>/dev/null || (cd /content/remote-cli && git pull)

import sys
sys.path.insert(0, "/content/remote-cli")

from client import start_in_background

start_in_background(
    proxy_url="wss://your-tunnel-domain.com/notebook",
    token="your-secure-secret-token",
    log_file="/content/runner.log"
)
print("Remote runner started in background on Colab!")
```

---

### 4.3. Cloud GPU VM Deployment (Lambda Labs, RunPod, Vast.ai)

On persistent cloud GPU nodes:

```bash
# Clone Remote-Cli
git clone https://github.com/avinash/Remote-Cli.git
cd Remote-Cli

# Install dependencies or use prebuilt binary
pip install websockets

# Start client as background process
nohup python3 client.py \
    --proxy "wss://your-tunnel-domain.com/notebook" \
    --token "your-secure-secret-token" \
    --notebook-id "gpu-worker-lambda-1" \
    --log-file "/var/log/remote-runner.log" \
    > /dev/null 2>&1 &
```

---

## 5. Controlling Remote Fleets with `remote-cli`

### 5.1. Configure CLI Environment
Set these variables in your local shell to avoid typing `--proxy-url` and `--token` every time:

```bash
export TESTING_PROXY_URL="wss://your-tunnel-domain.com/manager"
export TESTING_PROXY_TOKEN="your-secure-secret-token"
```

### 5.2. Common Fleet Workflows

```bash
# 1. Discover registered notebooks
./remote-cli --pretty list

# 2. Check remote hardware specs & GPU availability
./remote-cli --pretty system-info nb-3f9a12bc4e

# 3. Synchronize project code to remote working directory
./remote-cli push nb-3f9a12bc4e ./train.py /kaggle/working/train.py
./remote-cli push nb-3f9a12bc4e ./config.json /kaggle/working/config.json

# 4. Stream command output live to local terminal
./remote-cli shell nb-3f9a12bc4e "pip install -r requirements.txt" --stream

# 5. Launch non-blocking background jobs
./remote-cli shell nb-3f9a12bc4e "python3 train.py" --wait 5
# Output returns {"job_id": "job-7b8c9d0e1f", "status": "running", ...}

# 6. Check job logs periodically
./remote-cli --pretty job-logs nb-3f9a12bc4e job-7b8c9d0e1f --tail 50

# 7. Pull trained model weights or checkpoints back to local machine
./remote-cli pull nb-3f9a12bc4e /kaggle/working/checkpoint.pt ./checkpoint.pt
```

---

## 6. End-to-End Example: Deploying an LLM Inference Worker (vLLM)

Here is a practical workflow showing how to use Remote-Cli to deploy and monitor a vLLM inference worker on a remote Kaggle dual-T4 GPU notebook:

```bash
# 1. Verify notebook has connected
./remote-cli --pretty list

# 2. Check GPU VRAM and CUDA version
./remote-cli --pretty system-info nb-3f9a12bc4e

# 3. Upload worker code
./remote-cli push nb-3f9a12bc4e ./vllm_worker.py /kaggle/working/vllm_worker.py

# 4. Set model environment variables
./remote-cli env set nb-3f9a12bc4e MODEL_NAME "Qwen/Qwen2.5-7B-Instruct"
./remote-cli env set nb-3f9a12bc4e TENSOR_PARALLEL_SIZE "2"
./remote-cli env set nb-3f9a12bc4e GPU_MEMORY_UTILIZATION "0.85"

# 5. Start the inference worker as a managed process
./remote-cli --pretty subprocess nb-3f9a12bc4e "python3 /kaggle/working/vllm_worker.py"

# 6. Check process list to get the PID
./remote-cli --pretty ps nb-3f9a12bc4e

# 7. Follow server startup logs
./remote-cli --pretty logs nb-3f9a12bc4e --tail 100
```

---

## 7. Operational Best Practices & Troubleshooting

### 7.1. Timeout Handling in Shell Commands
- **Fast Commands**: Run `./remote-cli shell <ID> "<cmd>"` directly. It waits up to 30s by default.
- **Commands with Fixed Deadlines**: Add `--timeout <sec>` (e.g. `--timeout 120`). If the command exceeds 120 seconds, Remote-Cli kills the process tree and returns captured output.
- **Detached Long Operations**: Use `--wait <sec>` (e.g. `--wait 5`). If still running after 5s, Remote-Cli detaches the task and returns a `job_id` so your CLI does not hang.

### 7.2. Reconnecting Notebooks
If a Kaggle notebook restarts its kernel:
1. Ensure `.notebook_identity.json` was saved on a persistent path (such as `/kaggle/working`).
2. When the runner starts again, it will read `.notebook_identity.json` and reconnect under the exact same notebook ID.
3. Any commands issued while the notebook was restarting are buffered in the proxy's offline queue and executed automatically once the runner reconnects.
*(Note: Offline command queues are stored in proxy memory and do not survive a proxy server reboot).*

### 7.3. WebSocket Proxy Behind Cloudflare / Reverse Proxies
- Ensure your reverse proxy or tunnel allows **infinite/long WebSocket timeouts** (e.g. `proxy_read_timeout 3600s;` in Nginx).
- Remote-Cli sends application-level heartbeats every 10 seconds, which naturally keeps tunnel sockets alive through standard NATs and idle disconnect timers.

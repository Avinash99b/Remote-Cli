# Remote-Cli

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-16%20passed-brightgreen.svg)](test_system.py)
[![Architecture](https://img.shields.io/badge/Architecture-Async%20Proxy%20%7C%20Daemon%20Runner-orange.svg)](#architecture)

**Remote-Cli** is a resilient, production-ready remote execution and fleet management framework designed specifically for headless, ephemeral, or restricted environments such as **Kaggle notebooks**, **Google Colab**, **cloud GPU instances** (Lambda Labs, RunPod, Vast.ai), and isolated VMs.

It connects a central WebSocket proxy, lightweight remote runners, and a developer CLI to enable seamless file transfers, non-blocking background jobs, real-time log streaming, hardware telemetry, and remote Python execution—even across unstable networks and runner reconnects.

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Layout & Components](#project-layout--components)
- [Prerequisites & Installation](#prerequisites--installation)
- [Quick Start Guide](#quick-start-guide)
  - [1. Start the Proxy Server](#1-start-the-proxy-server)
  - [2. Start the Remote Runner (Client)](#2-start-the-remote-runner-client)
  - [3. Manage Remote Nodes via CLI](#3-manage-remote-nodes-via-cli)
- [Running in Kaggle Notebooks & Colab](#running-in-kaggle-notebooks--colab)
- [CLI Reference](#cli-reference)
  - [CLI Syntax & Conventions](#cli-syntax--conventions)
  - [Proxy & Fleet Discovery](#proxy--fleet-discovery)
  - [Remote Shell & Non-Blocking Background Jobs](#remote-shell--non-blocking-background-jobs)
  - [Long-Running Managed Subprocesses](#long-running-managed-subprocesses)
  - [Filesystem Operations & Transfers](#filesystem-operations--transfers)
  - [Persistent Python Execution](#persistent-python-execution)
  - [Environment Variables](#environment-variables)
  - [Telemetry & Node Control](#telemetry--node-control)
- [Core Capabilities Deep Dive](#core-capabilities-deep-dive)
  - [Fault Tolerance & Offline Command Queueing](#fault-tolerance--offline-command-queueing)
  - [Non-Blocking Concurrency & Job Isolation](#non-blocking-concurrency--job-isolation)
  - [Reliable Chunked File Transfers](#reliable-chunked-file-transfers)
  - [Hardware & GPU Telemetry](#hardware--gpu-telemetry)
- [Standalone Binary Build (PyInstaller)](#standalone-binary-build-pyinstaller)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [Environment Variables Configuration](#environment-variables-configuration)
- [Documentation Index](#documentation-index)

---

## Key Features

- **Bidirectional WebSocket Backbone**: Pure outbound connections from remote runners to the proxy; works behind NATs, firewalls, and restricted notebook containers without requiring open inbound ports on workers.
- **Offline Command Queueing**: When a remote node temporarily disconnects or kernel-restarts, commands are queued in-memory at the proxy and automatically flushed in FIFO order upon reconnection.
- **Non-Blocking Background Shell Jobs**: Shell commands run concurrently on dedicated threads. Stalled commands (such as interactive prompts or network hangs) never block heartbeat loops or subsequent commands.
- **Detached Execution & Job Tracking**: Long shell operations can automatically detach after a wait budget, returning a trackable `job_id` that can be monitored, polled, or terminated later.
- **Real-Time Log Streaming**: Stream stdout/stderr lines live as they are produced by remote commands without buffering delays.
- **Managed Long-Running Subprocesses**: Dedicated process supervision with bounded rolling logs (up to 100,000 lines), POSIX process-group tree termination, and process restarts.
- **Chunked File Transfer with Integrity Checking**: Upload and download large files in 1 MB chunks with streaming disk I/O, base64 encoding, and SHA-256 verification.
- **Deep Hardware & System Telemetry**: Live extraction of NVIDIA GPU metrics (VRAM usage, temperature, GPU compute utilization), CPU cores, RAM allocation, disk mount usage, and installed `pip` packages.
- **Persistent Python Execution**: Run arbitrary Python snippets remotely in a persistent namespace (`eval`) with captured stdout/stderr and return values.
- **Zero-Dependency Binary Distribution**: Build standalone single-file executables (`remote-cli`, `remote-cli-server`, `client`) with PyInstaller.

---

## Architecture

The system consists of three decoupled components communicating over a typed JSON WebSocket protocol:

```
┌─────────────────────────────────┐
│     Manager CLI / Agent         │
│  (remote-cli / manager.py)      │
└───────────────▲─────────────────┘
                │ WebSocket (/manager)
                │ [Commands, Streams, Responses]
┌───────────────▼─────────────────────────────────────────────────┐
│                    Central Proxy Server                         │
│               (remote-cli-server / server.py)                   │
│                                                                 │
│  • Routing & Request Correlation (UUIDs)                        │
│  • Connection Registry & Heartbeat Health Monitor               │
│  • Per-Notebook Offline Command Queues (In-memory FIFO)         │
│  • Pending Route TTL Garbage Collection                         │
└─────────────────────────────────▲───────────────────────────────┘
                                  │ WebSocket (/notebook)
                                  │ [Registration, Heartbeats, Jobs]
┌─────────────────────────────────▼───────────────────────────────┐
│                    Remote Runner Client                         │
│                  (client / client.py)                           │
│                                                                 │
│  • Persistent Identity (.notebook_identity.json)                │
│  • Non-blocking Command Dispatch Worker Thread                  │
│  • ManagedProcess Supervision (Process Groups, Deque Buffers)   │
│  • Streaming File Engine (1 MB Chunking, SHA-256 Checksums)     │
│  • System & Hardware Probing (nvidia-smi, /proc, pip)           │
└─────────────────────────────────────────────────────────────────┘
```

### Component Roles

1. **Proxy Server (`server.py` / `remote-cli-server`)**:
   - Listens on a single port (default: `8765`).
   - `/notebook`: Endpoint where runners connect, register with stable IDs and secrets, and exchange heartbeats.
   - `/manager`: Endpoint where CLI tools and agents issue commands and subscribe to execution streams.
   - Manages routing table, correlates requests to responses, queues commands for offline runners, and routes outputs back to the initiating manager.

2. **Runner Client (`client.py` / `client`)**:
   - Runs on the target worker (e.g., Kaggle notebook kernel, Colab, or remote GPU VM).
   - Establishes an outbound WebSocket connection to the proxy `/notebook`.
   - Persists node identity (`notebook_id` and token) in `.notebook_identity.json` so reconnects retain state.
   - Executes commands inside worker threads, monitors managed subprocesses, and streams console outputs.

3. **Manager CLI (`manager.py` / `remote-cli`)**:
   - Developer and agent command-line interface.
   - Emits structured JSON (with human-readable `--pretty` option).
   - Sends commands to `/manager` and streams remote outputs directly to the terminal.

---

## Project Layout & Components

| File | Purpose | Binary Target |
|---|---|---|
| `server.py` | Central WebSocket router, offline queue manager, and health tracker | `remote-cli-server` |
| `client.py` | Remote runner daemon for workers, notebooks, and background execution | `client` |
| `manager.py` | Unified CLI controller for users and automated AI agents | `remote-cli` |
| `protocol.py` | Shared message dataclasses, enums, payload builders, and chunk sizes | Shared module |
| `build.sh` | PyInstaller build script generating standalone single-binary executables | Executable generator |
| `test_system.py` | End-to-end integration and unit tests covering reconnects, queues, and streaming | Pytest suite |
| `deployment.md` | Practical deployment recipes for Kaggle, Colab, cloud VMs, and tunnels | Guide |
| `docs/` | Deep-dive documentation on architecture, protocol, and CLI commands | Reference manuals |

---

## Prerequisites & Installation

### Requirements
- **Python**: 3.9, 3.10, 3.11, or 3.12
- **Core Library**: `websockets` (works with modern `websockets>=12` and legacy versions)

### Python Virtual Environment Setup

```bash
# Clone the repository
git clone https://github.com/avinash/Remote-Cli.git
cd Remote-Cli

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required dependencies
pip install websockets pytest
```

### Precompiled Standalone Binaries
If you don't want Python runtime dependencies on client or server nodes, build single-file executables (run within an activated virtual environment):

```bash
source .venv/bin/activate
chmod +x build.sh
./build.sh
```
This produces `./remote-cli`, `./remote-cli-server`, and `./client` directly in the project root.

---

## Quick Start Guide

### 1. Start the Proxy Server

Run the server on a machine with a reachable IP or public tunnel (e.g., Cloudflare Tunnel or ngrok):

```bash
# Using Python
python3 server.py --host 0.0.0.0 --port 8765 --token my-secret-token

# Or using the standalone binary
./remote-cli-server --host 0.0.0.0 --port 8765 --token my-secret-token
```

### 2. Start the Remote Runner (Client)

On the remote worker machine or notebook:

```bash
# Using Python
python3 client.py --proxy ws://<PROXY_HOST>:8765/notebook --token my-secret-token

# Or using the standalone binary
./client --proxy ws://<PROXY_HOST>:8765/notebook --token my-secret-token
```
Upon registration, the runner outputs its assigned ID (e.g., `nb-1a2b3c4d5e`) and saves it to `.notebook_identity.json`.

### 3. Manage Remote Nodes via CLI

From your local terminal, interact with the connected fleet:

```bash
# Export proxy URL and token for convenience
export TESTING_PROXY_URL="ws://<PROXY_HOST>:8765/manager"
export TESTING_PROXY_TOKEN="my-secret-token"

# List all active runners
./remote-cli --pretty list

# Run a remote shell command
./remote-cli shell nb-1a2b3c4d5e "uname -a && nvidia-smi"

# Upload a local file to the remote node
./remote-cli push nb-1a2b3c4d5e ./local_script.py /kaggle/working/script.py

# Execute a local Python script remotely (uploads and runs in one step)
./remote-cli execute nb-1a2b3c4d5e ./local_script.py

# Download output or artifacts
./remote-cli pull nb-1a2b3c4d5e /kaggle/working/output.csv ./downloaded_output.csv
```

---

## Running in Kaggle Notebooks & Colab

Because Kaggle and Google Colab start in clean, ephemeral containers, you need to bootstrap `client.py` and start the runner in a background thread.

### Kaggle Notebook Bootstrap & Execution

Add this code to the first cell of your Kaggle notebook:

```python
# Cell 1: Bootstrap Remote-Cli and start daemon runner
!pip install -q websockets
!git clone https://github.com/avinash/Remote-Cli.git /kaggle/working/remote-cli 2>/dev/null || (cd /kaggle/working/remote-cli && git pull)

import sys
sys.path.insert(0, "/kaggle/working/remote-cli")

from client import start_in_background

# Start daemon runner in a background thread
thread = start_in_background(
    proxy_url="wss://your-tunnel-domain.com/notebook",
    token="my-secret-token",
    log_file="/kaggle/working/runner.log"
)
print("Remote-Cli runner successfully started in background!")
```

The runner persists its assigned ID and secret token to `/kaggle/working/.notebook_identity.json`. If you restart the notebook kernel, the client automatically re-authenticates with the same ID, recovering any queued commands.

---

## CLI Reference

### CLI Syntax & Conventions

The `remote-cli` tool adheres to standard POSIX subcommand conventions:

```
remote-cli [--proxy-url URL] [--token TOKEN] [--timeout SEC] [--pretty] <subcommand> [notebook_id] [args...]
```

- Pass global options (`--pretty`, `--timeout`, `--proxy-url`, `--token`) **before** the subcommand.
- Subcommands that target a remote runner expect `notebook_id` as their first positional argument.
- Fleet-level commands (`list`, `heartbeat`, `reconnect`) do not require a `notebook_id`.

---

### Proxy & Fleet Discovery

#### `list`
Lists all registered remote runners, connection health, and active workloads.
```bash
./remote-cli --pretty list
```

#### `heartbeat`
Checks proxy liveness, server uptime, and total connected nodes.
```bash
./remote-cli --pretty heartbeat
```

#### `reconnect`
Queries proxy router statistics, queue depths, and active manager connections.
```bash
./remote-cli --pretty reconnect
```

#### `info <notebook_id>`
Fetches detailed status of a specific runner, combining proxy metadata with live system telemetry if online.
```bash
./remote-cli --pretty info nb-8a9d12f3bc
```

---

### Remote Shell & Non-Blocking Background Jobs

#### `shell <notebook_id> <command> [options]`
Executes a bash command on the remote host.

Options:
- `--stream`: Streams console output lines live to `stderr` as they are emitted.
- `--timeout <seconds>`: Strict timeout limit. Kills the process tree and returns collected output if exceeded.
- `--wait <seconds>`: Non-blocking detachment budget (default: 30s). If the command runs longer than `--wait`, it detaches into a background job and returns a `job_id`.

```bash
# Synchronous run
./remote-cli shell nb-8a9d12f3bc "ls -lah /kaggle/working"

# Real-time streaming
./remote-cli shell nb-8a9d12f3bc "pip install torch torchvision" --stream

# Detached background execution (returns immediately after 5s with job_id)
./remote-cli shell nb-8a9d12f3bc "python3 train.py --epochs 100" --wait 5
```

#### `jobs <notebook_id>`
Lists all active, completed, or terminated shell background jobs.
```bash
./remote-cli --pretty jobs nb-8a9d12f3bc
```

#### `job-info <notebook_id> <job_id>`
Retrieves metadata, PID, exit code, start time, and runtime duration for a job.
```bash
./remote-cli --pretty job-info nb-8a9d12f3bc job-5f8e2a1b9c0d
```

#### `job-logs <notebook_id> <job_id> [--tail N]`
Fetches the latest output lines of a background job (default tail: 200).
```bash
./remote-cli --pretty job-logs nb-8a9d12f3bc job-5f8e2a1b9c0d --tail 50
```

#### `job-wait <notebook_id> <job_id> [--wait N] [--kill]`
Waits for an active background job to finish. If `--kill` is specified, terminates the job if it does not finish within the budget.
```bash
./remote-cli --pretty job-wait nb-8a9d12f3bc job-5f8e2a1b9c0d --wait 60
```

#### `job-terminate <notebook_id> <job_id>`
Sends `SIGTERM` (escalating to `SIGKILL`) to terminate a background job's process tree.
```bash
./remote-cli --pretty job-terminate nb-8a9d12f3bc job-5f8e2a1b9c0d
```

---

### Long-Running Managed Subprocesses

For dedicated long-running daemon services (like an LLM inference server or web API):

#### `subprocess <notebook_id> <command>`
Spawns a persistent managed subprocess with supervised output collection.
```bash
./remote-cli --pretty subprocess nb-8a9d12f3bc "python -m http.server 8080"
```

#### `ps <notebook_id>`
Lists all managed subprocesses, PIDs, uptime, and exit statuses.
```bash
./remote-cli --pretty ps nb-8a9d12f3bc
```

#### `logs <notebook_id> [process_id] [--tail N]`
Fetches logs for a specific managed process, or all managed processes if omitted.
```bash
./remote-cli --pretty logs nb-8a9d12f3bc my-proc-id --tail 100
```

#### `restart <notebook_id> <process_id>`
Restarts a managed process and increments its restart counter.
```bash
./remote-cli --pretty restart nb-8a9d12f3bc my-proc-id
```

#### `terminate <notebook_id> <process_id>`
Stops a managed process and cleans up its POSIX process group.
```bash
./remote-cli --pretty terminate nb-8a9d12f3bc my-proc-id
```

---

### Filesystem Operations & Transfers

#### `push <notebook_id> <local_path> [remote_path]`
Uploads a local file to the remote runner in chunked 1 MB fragments with streaming disk I/O.
```bash
./remote-cli push nb-8a9d12f3bc ./model.pt /kaggle/working/model.pt
```

#### `pull <notebook_id> <remote_path> [local_path]`
Downloads a remote file with chunked transfer and validates its SHA-256 integrity hash upon completion.
```bash
./remote-cli pull nb-8a9d12f3bc /kaggle/working/training.log ./training.log
```

#### `execute <notebook_id> <local_path> [args]`
Uploads a local Python script to the remote node and immediately executes it via `python3 <filename> [args]`.
```bash
./remote-cli execute nb-8a9d12f3bc ./eval.py "--batch-size 32"
```

*Note*: To execute a script that already exists on the remote node without re-uploading, use `shell`:
```bash
./remote-cli shell nb-8a9d12f3bc "python3 /kaggle/working/existing_script.py"
```

#### `ls <notebook_id> [path] [-r]`
Lists remote files and directories (supports recursive `-r`).
```bash
./remote-cli --pretty ls nb-8a9d12f3bc /kaggle/working
```

#### `cp <notebook_id> <src> <dst>`
Copies a remote file or folder.
```bash
./remote-cli cp nb-8a9d12f3bc /kaggle/working/model.pt /kaggle/working/backup.pt
```

#### `mv <notebook_id> <src> <dst>`
Moves or renames a remote file or folder.
```bash
./remote-cli mv nb-8a9d12f3bc /kaggle/working/old_name.py /kaggle/working/new_name.py
```

#### `pwd <notebook_id>` / `cd <notebook_id> <path>`
Inspects or changes the remote working directory.
```bash
./remote-cli pwd nb-8a9d12f3bc
./remote-cli cd nb-8a9d12f3bc /kaggle/working
```

#### `mkdir <notebook_id> <path>` / `rm <notebook_id> <path>`
Creates directories or deletes files/directories (recursive, idempotent).
```bash
./remote-cli mkdir nb-8a9d12f3bc /kaggle/working/checkpoints
./remote-cli rm nb-8a9d12f3bc /kaggle/working/temp_file.txt
```

---

### Persistent Python Execution

#### `eval <notebook_id> "<code>" [--capture-locals]`
Executes Python code directly inside the runner's persistent execution namespace (`__remote_testing__`).

```bash
# Evaluate an expression and inspect result
./remote-cli --pretty eval nb-8a9d12f3bc "import torch; torch.cuda.get_device_name(0)"

# Set persistent state across invocations
./remote-cli eval nb-8a9d12f3bc "x = 42"
./remote-cli eval nb-8a9d12f3bc "x * 2"  # Returns 84
```

---

### Environment Variables

Environment variables modified through `remote-cli` are stored in a runtime overlay and injected into all future commands and processes:

```bash
# List remote environment variables
./remote-cli --pretty env list nb-8a9d12f3bc

# Set an environment variable
./remote-cli env set nb-8a9d12f3bc HF_TOKEN "hf_abcdef123456"

# Get an environment variable
./remote-cli env get nb-8a9d12f3bc HF_TOKEN

# Unset an environment variable
./remote-cli env unset nb-8a9d12f3bc HF_TOKEN
```

---

### Telemetry & Node Control

#### `ping <notebook_id>`
Pings the remote runner; measures round-trip latency and remote uptime.
```bash
./remote-cli --pretty ping nb-8a9d12f3bc
```

#### `system-info <notebook_id>`
Extracts detailed live system telemetry:
- **GPU Info**: Name, VRAM total/free/used, temperature, compute utilization percentage.
- **CUDA Info**: CUDA driver and toolkit versions.
- **RAM & Disk**: Total, available, and percentage utilized across mounts (`/`, `/kaggle/working`).
- **Python & OS**: Kernel version, architecture, CPU count, and installed `pip` packages.

```bash
./remote-cli --pretty system-info nb-8a9d12f3bc
```

#### `shutdown <notebook_id> [--no-terminate-processes]`
Gracefully shuts down the remote runner daemon.
```bash
./remote-cli shutdown nb-8a9d12f3bc
```

---

## Core Capabilities Deep Dive

### Fault Tolerance & Offline Command Queueing

In remote training environments (like Kaggle or Colab), network connections frequently flap or undergo brief outages:

1. **Heartbeat Detection**: The client sends periodic heartbeats (default every 10s) with active workload stats. If 3 consecutive heartbeats are unacknowledged, the runner resets its socket and reconnects.
2. **Server-Side Queueing**: If a command is issued to a notebook that is currently disconnected, the proxy holds the command in an in-memory FIFO queue (up to 100 commands by default) and immediately returns a `COMMAND_QUEUED` message to the manager.
3. **Automatic Queue Flushing**: As soon as the runner reconnects with its persistent identity and token, the proxy flushes all queued commands to the runner in exact sequence.
4. **Manager Connection Resilience**: If a runner reconnects while a long command is executing, the response is delivered back to the originating manager using correlated request IDs.
*(Note: Offline command queues are held in proxy memory and do not survive a restart of the proxy process).*

### Non-Blocking Concurrency & Job Isolation

Previous remote executors blocked the message receive loop while executing shell tasks. Remote-Cli completely decouples command dispatch from execution:

- Every incoming command is popped from an internal `command_queue` and assigned a dedicated worker thread (`cmd-<uuid>`).
- If an interactive command blocks indefinitely (e.g. waiting for `yes/no` or SSH host-key prompt), the client **continues processing subsequent commands** (such as `pwd`, `system-info`, or `job-terminate`).
- Shell executions automatically create a `ManagedProcess` with non-blocking stream pump threads reading stdout and stderr into memory-safe circular deques.
- POSIX process groups are created with `start_new_session=True`, ensuring child processes and forks are completely cleaned up upon termination.

### Reliable Chunked File Transfers

Transferring multi-gigabyte models or large datasets over WebSockets can saturate memory or trigger frame limits:

- Transfers are partitioned into **1 MB (`1024 * 1024`) binary chunks** encoded in base64.
- Transfers stream directly to and from disk rather than buffering entire files into contiguous RAM.
- Downloads compute an end-to-end **SHA-256 checksum**, which the manager compares against the remote hash before replacing the destination file.
- Partial upload buffers have automatic 10-minute TTL cleanup to prevent memory leaks on aborted transfers.

### Hardware & GPU Telemetry

Remote-Cli provides first-class support for GPU infrastructure:
- Direct query to `nvidia-smi` formatting GPU index, name, memory allocation, temperature, and compute load.
- Memory parsing via `/proc/meminfo` to accurately compute available vs cached memory.
- Filesystem inspection using `shutil.disk_usage` across root and working paths.

---

## Standalone Binary Build (PyInstaller)

To deploy without installing Python packages on target environments, compile single-binary executables using `build.sh`:

```bash
./build.sh
```

This uses PyInstaller with `--onefile --clean` to produce:
1. `remote-cli`: The CLI management client.
2. `remote-cli-server`: The proxy server daemon.
3. `client`: The runner daemon to deploy on Kaggle or remote VMs.

The binaries are placed in both `./dist/` and the project root directory.

---

## Testing & Quality Assurance

Remote-Cli includes an extensive test suite verifying protocol parsing, process supervision, streaming, disconnect resilience, command queuing, chunked transfers, and concurrency:

```bash
# Run the full test suite
pytest -v
```

The test suite covers:
- Protocol serialization, deserialization, and schema robustness against unknown fields.
- Subprocess execution, streaming callbacks, and process tree termination.
- Proxy routing, offline command queueing, and queue flushing on reconnect.
- Multi-chunk binary file upload/download with SHA-256 integrity verification.
- Directory deletion safety (verifying non-recursive `rm` on directory fails safely).
- Persistent namespace Python execution (`python_exec`).
- Environment variable management (`env set/get/unset`) and process overlay.
- Non-blocking execution of concurrent commands during long sleep/blocking tasks.
- Background job detachment, job status polling, and log retrieval.
- Memory leak prevention and pending route TTL cleanup.
- CLI argument parsing across all subcommands.

---

## Environment Variables Configuration

| Variable | Target Component | Default Value | Description |
|---|---|---|---|
| `TESTING_PROXY_URL` | `manager.py` / `remote-cli` | `ws://localhost:8765/manager` | Default proxy URL for the CLI |
| `TESTING_PROXY_TOKEN` | `manager.py` / `remote-cli` | `None` | Shared authorization token |

---

## Documentation Index

- [**Deployment Guide (`deployment.md`)**](deployment.md): Complete setup for Kaggle, Colab, Cloud GPUs (RunPod, Lambda), ngrok/Cloudflare tunnels, systemd services, and production vLLM workers.
- [**Architecture Specification (`docs/ARCHITECTURE.md`)**](docs/ARCHITECTURE.md): Detailed internals, state machines, threading architecture, and failure recovery.
- [**Protocol Reference (`docs/PROTOCOL.md`)**](docs/PROTOCOL.md): Wire format, JSON message envelopes, MessageType enums, and sequence diagrams.
- [**CLI Reference Manual (`docs/CLI_REFERENCE.md`)**](docs/CLI_REFERENCE.md): Comprehensive manual for every command, argument, flag, and JSON response.

# Remote-Cli Command-Line Reference Manual

This document provides exhaustive documentation for all command-line executables and subcommands in the **Remote-Cli** suite.

---

## 1. `remote-cli` (Fleet Manager CLI)

### 1.1. Syntax & Global Flags

```
remote-cli [--proxy-url PROXY_URL] [--token TOKEN] [--timeout TIMEOUT] [--pretty] <subcommand> [args...]
```

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--proxy-url` | `TESTING_PROXY_URL` | `ws://localhost:8765/manager` | Proxy WebSocket URL for managers. |
| `--token` | `TESTING_PROXY_TOKEN` | `None` | Shared secret authentication token. |
| `--timeout` | None | `120.0` | Default timeout budget in seconds for command response. |
| `--pretty` | None | `False` | Formats output JSON with 2-space indentation. |

### 1.2. Exit Codes
- `0`: Operation succeeded (`"ok": true`).
- `1`: Operation failed (`"ok": false`), remote execution failed, or connection timed out.

---

### 1.3. Subcommand Reference

---

#### `list`
Query the proxy server for all connected and registered runners.

**Arguments**: None.

**Example**:
```bash
remote-cli --pretty list
```
**Response**:
```json
{
  "ok": true,
  "command": "list",
  "result": {
    "success": true,
    "notebooks": [
      {
        "notebook_id": "nb-9f8e7d6c5b",
        "status": "connected",
        "hostname": "kaggle-worker-1",
        "connected_at": 1729081200.0,
        "age_seconds": 34.2,
        "last_heartbeat_age_seconds": 1.5,
        "queued_commands": 0,
        "workload": {
          "running_processes": 0,
          "total_processes": 0,
          "commands_served": 12
        }
      }
    ]
  }
}
```

---

#### `heartbeat`
Verifies proxy server liveness and overall connection metrics.

**Arguments**: None.

**Example**:
```bash
remote-cli --pretty heartbeat
```
**Response**:
```json
{
  "ok": true,
  "command": "heartbeat",
  "result": {
    "success": true,
    "status": "ok",
    "uptime_seconds": 1520.4,
    "notebooks_total": 3,
    "notebooks_connected": 2
  }
}
```

---

#### `reconnect`
Inspects low-level proxy router statistics and connection inventories.

**Arguments**: None.

**Example**:
```bash
remote-cli --pretty reconnect
```

---

#### `info <notebook_id>`
Retrieves static connection metadata from the proxy and live telemetry from the runner if currently online.

**Arguments**:
- `notebook_id`: Target runner ID (e.g. `nb-9f8e7d6c5b`).

**Example**:
```bash
remote-cli --pretty info nb-9f8e7d6c5b
```

---

#### `push <notebook_id> <local_path> [remote_path]`
Uploads a local file to the remote host. The file is split into 1 MB chunks and uploaded via WebSocket frames.

**Arguments**:
- `notebook_id`: Target runner ID.
- `local_path`: Local file to upload.
- `remote_path` *(Optional)*: Destination path on the remote host (defaults to filename in remote working directory).

**Example**:
```bash
remote-cli push nb-9f8e7d6c5b ./train.py /kaggle/working/train.py
```
**Response**:
```json
{
  "ok": true,
  "command": "push",
  "result": {
    "success": true,
    "complete": true,
    "path": "/kaggle/working/train.py",
    "bytes": 48201
  }
}
```

---

#### `pull <notebook_id> <remote_path> [local_path]`
Downloads a remote file with chunked transfer and validates its SHA-256 integrity hash upon completion.

**Arguments**:
- `notebook_id`: Target runner ID.
- `remote_path`: Path of remote file to download.
- `local_path` *(Optional)*: Destination path on the local filesystem.

**Example**:
```bash
remote-cli pull nb-9f8e7d6c5b /kaggle/working/metrics.json ./metrics.json
```
**Response**:
```json
{
  "ok": true,
  "command": "pull",
  "result": {
    "success": true,
    "local_path": "./metrics.json",
    "remote_path": "/kaggle/working/metrics.json",
    "bytes": 1024,
    "sha256": "4a5b6c7d8e...",
    "integrity_ok": true
  }
}
```

---

#### `execute <notebook_id> <local_path> [args]`
Convenience wrapper that uploads a local Python script to the remote host and immediately executes it via `python3 <script> [args]`.

**Arguments**:
- `notebook_id`: Target runner ID.
- `local_path`: Local Python script to upload and run.
- `args` *(Optional)*: Command-line arguments passed to the script.

**Example**:
```bash
remote-cli execute nb-9f8e7d6c5b ./eval.py "--model-path /kaggle/working/weights"
```

*Note*: If the script is already present on the remote node, run it directly via `shell`:
```bash
remote-cli shell nb-9f8e7d6c5b "python3 /kaggle/working/eval.py --model-path /kaggle/working/weights"
```

---

#### `shell <notebook_id> <command> [options]`
Runs a shell command on the remote host inside a dedicated subprocess.

**Arguments**:
- `notebook_id`: Target runner ID.
- `command`: Bash command string.

**Options**:
- `--stream`: Streams console output lines to local `stderr` in real time.
- `--timeout <float>`: Hard timeout limit in seconds. Kills the process tree if exceeded.
- `--wait <float>`: Soft detachment budget (default: 30s). If still running when wait expires, returns a detached `job_id`.

**Examples**:
```bash
# Direct synchronous run
remote-cli shell nb-9f8e7d6c5b "ls -la"

# Real-time streaming
remote-cli shell nb-9f8e7d6c5b "pip install transformers" --stream

# Background detachment
remote-cli shell nb-9f8e7d6c5b "python3 long_task.py" --wait 5
```

---

#### `jobs <notebook_id>`
Lists all background shell jobs (running, completed, failed, or terminated).

**Arguments**:
- `notebook_id`: Target runner ID.

**Example**:
```bash
remote-cli --pretty jobs nb-9f8e7d6c5b
```

---

#### `job-info <notebook_id> <job_id>`
Retrieves process metadata, PID, exit code, start time, and runtime duration for a job.

**Arguments**:
- `notebook_id`: Target runner ID.
- `job_id`: Job ID (e.g. `job-5a4b3c2d1e0f`).

---

#### `job-logs <notebook_id> <job_id> [--tail N]`
Fetches captured stdout and stderr lines for a specific background job.

**Options**:
- `--tail <int>`: Number of trailing lines to return (default: 200).

**Example**:
```bash
remote-cli --pretty job-logs nb-9f8e7d6c5b job-5a4b3c2d1e0f --tail 100
```

---

#### `job-wait <notebook_id> <job_id> [options]`
Waits for an active background job to finish.

**Options**:
- `--wait <float>`: Seconds to wait before returning (default: 30.0).
- `--kill`: Terminate the job if it has not completed when the wait budget expires.

**Example**:
```bash
remote-cli --pretty job-wait nb-9f8e7d6c5b job-5a4b3c2d1e0f --wait 60
```

---

#### `job-terminate <notebook_id> <job_id>`
Terminates a background job and kills its process tree (SIGTERM with SIGKILL fallback).

**Arguments**:
- `notebook_id`: Target runner ID.
- `job_id`: Job ID to terminate.

---

#### `subprocess <notebook_id> <command>`
Spawns a long-running managed service with persistent circular logging buffers.

**Arguments**:
- `notebook_id`: Target runner ID.
- `command`: Command to run.

**Example**:
```bash
remote-cli --pretty subprocess nb-9f8e7d6c5b "python -m vllm.entrypoints.openai.api_server"
```

---

#### `ps <notebook_id>`
Lists all active and managed subprocesses on the runner.

**Arguments**:
- `notebook_id`: Target runner ID.

---

#### `logs <notebook_id> [process_id] [--tail N]`
Retrieves rolling logs from managed subprocesses.

**Arguments**:
- `process_id` *(Optional)*: Target process ID. If omitted, returns logs for all managed processes.
- `--tail <int>`: Trailing lines to fetch (default: 100).

---

#### `restart <notebook_id> <process_id>`
Restarts a managed process and increments its restart counter.

**Arguments**:
- `notebook_id`: Target runner ID.
- `process_id`: Managed process ID.

**Example**:
```bash
remote-cli --pretty restart nb-9f8e7d6c5b my-process-id
```

---

#### `terminate <notebook_id> <process_id>`
Stops a managed process and tears down its POSIX process group.

**Arguments**:
- `notebook_id`: Target runner ID.
- `process_id`: Managed process ID.

---

#### `eval <notebook_id> "<code>" [--capture-locals]`
Executes arbitrary Python code remotely inside the persistent execution namespace (`__remote_testing__`).

**Arguments**:
- `notebook_id`: Target runner ID.
- `code`: Python code string.

**Options**:
- `--capture-locals`: Include local namespace variables in the returned JSON.

**Example**:
```bash
remote-cli --pretty eval nb-9f8e7d6c5b "import torch; torch.cuda.get_device_name(0)"
```

---

#### `ls <notebook_id> [path] [-r]`
Lists remote files and directories.

**Arguments**:
- `notebook_id`: Target runner ID.
- `path` *(Optional)*: Directory path to list (defaults to `.`).

**Options**:
- `-r`, `--recursive`: Recursively traverse subdirectories.

**Example**:
```bash
remote-cli --pretty ls nb-9f8e7d6c5b /kaggle/working
```

---

#### `cp <notebook_id> <src> <dst>`
Copies a remote file or folder.

**Arguments**:
- `notebook_id`: Target runner ID.
- `src`: Remote source path.
- `dst`: Remote destination path.

---

#### `mv <notebook_id> <src> <dst>`
Moves or renames a remote file or folder.

**Arguments**:
- `notebook_id`: Target runner ID.
- `src`: Remote source path.
- `dst`: Remote destination path.

---

#### `pwd <notebook_id>`
Prints the current working directory of the remote runner.

---

#### `cd <notebook_id> <path>`
Changes the working directory of the remote runner (updates internal state and `os.chdir`).

---

#### `mkdir <notebook_id> <path>`
Creates a directory on the remote runner (recursive, idempotent).

---

#### `rm <notebook_id> <path>`
Recursively deletes a remote file or folder.

---

#### `ping <notebook_id>`
Sends a ping to the remote runner and returns its current uptime and timestamp.

---

#### `system-info <notebook_id>`
Extracts detailed live system telemetry:
- NVIDIA GPU metrics (index, name, VRAM used/free/total, temperature, compute utilization percentage).
- CUDA version.
- RAM utilization from `/proc/meminfo`.
- Disk space across mounts (`/`, `/kaggle/working`).
- Complete list of installed `pip` packages.

---

#### `shutdown <notebook_id> [--no-terminate-processes]`
Gracefully shuts down the remote runner daemon.

**Options**:
- `--no-terminate-processes`: Leave managed background processes running when the daemon exits.

**Example**:
```bash
remote-cli shutdown nb-9f8e7d6c5b
```

---

#### `env list <notebook_id>`
Lists active environment variables, including runtime overlay modifications.

**Example**:
```bash
remote-cli --pretty env list nb-9f8e7d6c5b
```

---

#### `env get <notebook_id> <key>`
Reads a specific environment variable from the remote host.

**Example**:
```bash
remote-cli env get nb-9f8e7d6c5b HF_TOKEN
```

---

#### `env set <notebook_id> <key> <value>`
Sets an environment variable in both `os.environ` and the persistent overlay for all future processes.

**Example**:
```bash
remote-cli env set nb-9f8e7d6c5b HF_TOKEN "hf_12345"
```

---

#### `env unset <notebook_id> <key>`
Removes an environment variable from `os.environ` and the overlay.

**Example**:
```bash
remote-cli env unset nb-9f8e7d6c5b HF_TOKEN
```

---

## 2. `remote-cli-server` (Proxy Server)

### Syntax & Options

```
remote-cli-server [--host HOST] [--port PORT] [--token TOKEN]
                  [--heartbeat-timeout SEC] [--queue-max N]
                  [--monitor-interval SEC] [--log-level LEVEL] [--log-file PATH]
```

| Option | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Bind IP interface. |
| `--port` | `8765` | WebSocket listening port. |
| `--token` | `None` | Shared authorization secret (matches `?token=...`). |
| `--heartbeat-timeout` | `60.0` | Inactivity threshold before marking runner disconnected. |
| `--queue-max` | `100` | Maximum offline command queue capacity per notebook (in-memory). |
| `--monitor-interval` | `5.0` | Interval between heartbeat watchdog checks. |
| `--log-level` | `INFO` | Console & file logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `--log-file` | `None` | Rotating log file destination (10 MB files, 5 backups). |

---

## 3. `client` (Remote Runner)

### Syntax & Options

```
client --proxy PROXY_URL [--token TOKEN] [--notebook-id ID]
       [--identity-file PATH] [--heartbeat-interval SEC]
       [--reconnect-delay SEC] [--max-heartbeat-misses N]
       [--log-level LEVEL] [--log-file PATH]
```

| Option | Default | Description |
|---|---|---|
| `--proxy` | *Required* | Proxy WebSocket URL (e.g. `wss://tunnel.domain.com/notebook`). |
| `--token` | `None` | Authentication token matching proxy. |
| `--notebook-id` | `None` | Preferred runner ID (defaults to auto-assignment or identity file). |
| `--identity-file` | `.notebook_identity.json` | Path where persistent notebook ID & secret are stored. |
| `--heartbeat-interval` | `10.0` | Seconds between periodic heartbeat transmissions. |
| `--reconnect-delay` | `5.0` | Fixed retry delay in seconds between reconnection attempts. |
| `--max-heartbeat-misses`| `3` | Heartbeat misses before forcing socket reset. |
| `--log-level` | `INFO` | Console & file logging level. |
| `--log-file` | `None` | Rotating log file path. |

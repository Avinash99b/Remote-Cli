# Remote-Cli Protocol Specification

This document defines the wire format, message envelopes, command schemas, and sequence flows used by the **Remote-Cli** framework.

---

## 1. Transport & Framing

- **Transport**: WebSocket protocol (RFC 6455) using `ws://` (unencrypted) or `wss://` (TLS encrypted).
- **Framing**: UTF-8 encoded text frames carrying serialized JSON objects.
- **Chunk Size**: Binary transfers (such as file uploads and downloads) are partitioned into **1,048,576 bytes (1 MB)** chunks and base64-encoded.
- **Keep-Alive**: WebSockets use both RFC 6455 Ping/Pong frames (every 10s–20s) and application-level `HEARTBEAT` messages.

---

## 2. Base Message Envelope

All communication between Manager, Proxy, and Runner wraps payloads in a standard envelope:

```json
{
  "message_id": "8e3b4a21-9d2a-4c28-98e3-0d3f281e847c",
  "message_type": "command",
  "timestamp": "2026-10-16T12:00:00.000000",
  "correlation_id": "c1a2b3c4-d5e6-7f8a-9b0c-1d2e3f4a5b6c",
  "payload": {}
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `message_id` | `string` (UUIDv4) | Unique identifier for every message instance. |
| `message_type` | `string` (Enum) | Defines the semantic purpose of the envelope (see `MessageType`). |
| `timestamp` | `string` (ISO-8601 UTC) | Message creation timestamp. |
| `correlation_id` | `string` or `null` | References the originating `message_id` for responses, streams, and acks. |
| `payload` | `object` | Command parameters or result dictionary. |

---

## 3. MessageType Enums

| Enum Value | Purpose | Sender -> Receiver |
|---|---|---|
| `register` | Runner registration and capability handshake | Runner -> Proxy |
| `register_ack` | Registration confirmation with assigned ID and token | Proxy -> Runner / Manager |
| `heartbeat` | Health check & workload report (every 10s) | Runner / Manager -> Proxy |
| `heartbeat_ack` | Heartbeat receipt confirmation | Proxy -> Runner / Manager |
| `manager_hello` | Manager connection initialization | Manager -> Proxy |
| `command` | Action request targeted at a runner or proxy | Manager -> Proxy -> Runner |
| `command_response`| Final execution result | Runner -> Proxy -> Manager |
| `command_stream` | Incremental real-time output line or download chunk | Runner -> Proxy -> Manager |
| `command_queued` | Notification that command is queued in proxy memory while runner is offline (non-durable across proxy restarts) | Proxy -> Manager |
| `error` | Protocol or routing error | Proxy / Runner -> Manager |

---

## 4. Runner Registration & Authentication

### 4.1. Runner -> Proxy (`register`)
Sent by the runner as its first WebSocket message immediately upon connecting to `/notebook`:

```json
{
  "message_type": "register",
  "payload": {
    "notebook_id": "nb-abc1234567",
    "secret": "b5a93d2c8f1e",
    "reconnect": true,
    "info": {
      "hostname": "kaggle-gpu-worker-1",
      "python_version": "3.10.12",
      "os": "Linux-5.15.0-generic-x86_64",
      "arch": "x86_64",
      "cpu_count": 4,
      "cwd": "/kaggle/working"
    }
  }
}
```
*Note*: If `notebook_id` is `"auto"` or null, the proxy assigns a new ID and random secret.

### 4.2. Proxy -> Runner (`register_ack`)
```json
{
  "message_type": "register_ack",
  "correlation_id": "<register_message_id>",
  "payload": {
    "notebook_id": "nb-abc1234567",
    "secret": "b5a93d2c8f1e",
    "resumed": true,
    "status": "connected",
    "queued_flushed": 2,
    "server_time": 1729080005.123
  }
}
```

---

## 5. Command Payloads (`CommandType`)

Commands are passed inside `payload.command` of a `MessageType.COMMAND` envelope.

### 5.1. Shell & Job Operations

#### `shell_exec`
Executes a bash shell command with optional streaming and wait/timeout budgets.
```json
{
  "command": "shell_exec",
  "notebook_id": "nb-abc1234567",
  "shell_command": "pip install torchvision",
  "cwd": "/kaggle/working",
  "env": {"CUDA_VISIBLE_DEVICES": "0"},
  "timeout": 60.0,
  "wait": 30.0,
  "stream": true
}
```

**Streamed Message (`command_stream`)**:
```json
{
  "message_type": "command_stream",
  "correlation_id": "<shell_exec_message_id>",
  "payload": {
    "kind": "shell_line",
    "stream": "stdout",
    "line": "Collecting torchvision..."
  }
}
```

**Final Response (`command_response`)**:
```json
{
  "message_type": "command_response",
  "correlation_id": "<shell_exec_message_id>",
  "payload": {
    "success": true,
    "exit_code": 0,
    "execution_time": 4.12,
    "job_id": "job-f8a1b2c3d4e5",
    "stdout": "Collecting torchvision...\nSuccessfully installed...",
    "stderr": ""
  }
}
```

#### `job_list`
Lists all background shell jobs.
```json
{
  "command": "job_list",
  "notebook_id": "nb-abc1234567"
}
```

#### `job_logs`
```json
{
  "command": "job_logs",
  "notebook_id": "nb-abc1234567",
  "job_id": "job-f8a1b2c3d4e5",
  "tail": 100
}
```

#### `job_wait`
Waits for an active background job to finish.
```json
{
  "command": "job_wait",
  "notebook_id": "nb-abc1234567",
  "job_id": "job-f8a1b2c3d4e5",
  "timeout": 30.0,
  "kill_on_timeout": false
}
```

#### `job_terminate`
Kills the background job's process tree.
```json
{
  "command": "job_terminate",
  "notebook_id": "nb-abc1234567",
  "job_id": "job-f8a1b2c3d4e5",
  "signal": 15
}
```

---

### 5.2. File Operations

#### `file_upload` (Chunked)
Uploads a file chunk. When `chunk_index == total_chunks - 1`, the runner joins fragments and writes to disk.
```json
{
  "command": "file_upload",
  "notebook_id": "nb-abc1234567",
  "remote_path": "/kaggle/working/dataset.zip",
  "encoding": "base64",
  "content": "<base64-encoded-data>",
  "chunk_index": 0,
  "total_chunks": 5
}
```

#### `file_download`
Initiates a download. The runner streams chunks back via `command_stream` with `kind: "download_chunk"`, followed by a final `command_response` containing the file's SHA-256 digest:
```json
{
  "command": "file_download",
  "notebook_id": "nb-abc1234567",
  "remote_path": "/kaggle/working/model.pt"
}
```

**Download Chunk (`command_stream`)**:
```json
{
  "message_type": "command_stream",
  "correlation_id": "<file_download_message_id>",
  "payload": {
    "kind": "download_chunk",
    "path": "/kaggle/working/model.pt",
    "chunk_index": 0,
    "total_chunks": 4,
    "content": "<base64-encoded-data>",
    "encoding": "base64",
    "complete": false
  }
}
```

**Download Final Response (`command_response`)**:
```json
{
  "message_type": "command_response",
  "correlation_id": "<file_download_message_id>",
  "payload": {
    "kind": "download_complete",
    "path": "/kaggle/working/model.pt",
    "bytes": 4194304,
    "total_chunks": 4,
    "complete": true,
    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  }
}
```

---

### 5.3. Remote Python Execution (`python_exec`)

Executes Python code directly in the runner's persistent execution namespace (`__remote_testing__`).

```json
{
  "command": "python_exec",
  "notebook_id": "nb-abc1234567",
  "code": "import torch; __result__ = torch.cuda.is_available()",
  "capture_locals": false
}
```

**Response**:
```json
{
  "message_type": "command_response",
  "payload": {
    "success": true,
    "result": true,
    "stdout": "",
    "stderr": "",
    "exception": null,
    "persistent_namespace_keys": ["__builtins__", "__name__", "torch", "__result__"]
  }
}
```

---

### 5.4. System Information (`system_info`)

```json
{
  "command": "system_info",
  "notebook_id": "nb-abc1234567"
}
```

**Response Payload**:
```json
{
  "success": true,
  "hostname": "kaggle-worker-1",
  "python_version": "3.10.12",
  "cuda_version": "12.2",
  "gpus": [
    {
      "index": 0,
      "name": "Tesla T4",
      "memory_total_mb": 15360,
      "memory_free_mb": 14200,
      "memory_used_mb": 1160,
      "temperature_c": 45,
      "utilization_percent": 12
    }
  ],
  "ram": {
    "total_bytes": 33740263424,
    "available_bytes": 31250186240,
    "used_bytes": 2490077184,
    "percent_used": 7.38
  },
  "disk": [
    {
      "mount": "/kaggle/working",
      "total_bytes": 20436856832,
      "used_bytes": 104857600,
      "free_bytes": 20331999232
    }
  ],
  "installed_packages": [
    "torch==2.1.2",
    "transformers==4.36.0",
    "websockets==12.0"
  ]
}
```

---

### 5.5. Proxy Level Commands

Handled directly by the proxy without forwarding to any runner:

| Command | Payload Parameters | Returns |
|---|---|---|
| `proxy_list` | None | Array of all registered runners and connection metadata |
| `proxy_info` | `{"notebook_id": "..."}` | Detailed summary for a single notebook |
| `proxy_heartbeat` | None | Proxy uptime, total registered nodes, active connections |
| `proxy_stats` | None | Command counters, active manager connections, pending route map size |

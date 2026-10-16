# Remote-Cli Architecture Specification

This document details the internal design, concurrency models, state machines, and resilience mechanisms of the **Remote-Cli** system.

---

## 1. High-Level System Topology

Remote-Cli is designed for **asymmetric client-server networking**:
- Remote compute environments (e.g., Kaggle kernels, Colab notebooks, worker nodes behind firewalls) cannot accept inbound connections.
- They establish persistent **outbound WebSocket connections** to a central, publicly accessible proxy.
- Developers, automation scripts, and AI agents communicate with the proxy via a separate control path.

```
+-------------------------------------------------------------------------+
|                              Manager CLI                                |
|  - InferenceClientManager (asyncio)                                     |
|  - CLI parser & command dispatcher                                      |
|  - stdout/stderr stream consumer                                        |
|  - 1MB chunked file packager & SHA-256 verifier                         |
+------------------------------------+------------------------------------+
                                     |
                                     | WebSocket /manager
                                     v
+------------------------------------+------------------------------------+
|                         Central Proxy Server                            |
|  - TestingProxy (asyncio event loop)                                    |
|  - Registry of NotebookConnection instances                             |
|  - Correlation ID table: correlation_id -> (manager_id, timestamp)      |
|  - Offline Command Queue: pending_queue per notebook                    |
|  - Heartbeat Watchdog & Monitor Loop (every 5.0s)                       |
|  - Memory GC for stale pending routes                                   |
+------------------------------------+------------------------------------+
                                     |
                                     | WebSocket /notebook
                                     v
+------------------------------------+------------------------------------+
|                         Remote Runner Client                            |
|  - KaggleNotebookRunner (sync WebSocket + background threads)           |
|  - Identity store (.notebook_identity.json)                             |
|  - Background Heartbeat Thread (interval: 10s, max 3 misses)            |
|  - Internal Command Queue & Worker Dispatcher Thread                    |
|  - Per-command execution threads (cmd-<uuid>)                           |
|  - ManagedProcess supervision (POSIX session leader, reader pumps)      |
|  - In-memory circular log buffers (collections.deque, max 100,000 lines)|
|  - Persistent Python namespace (__remote_testing__)                     |
+-------------------------------------------------------------------------+
```

---

## 2. Component Concurrency & Threading Models

### 2.1. Central Proxy (`server.py`)
The proxy runs inside a single Python `asyncio` event loop:
- **Connection Handlers**: Each incoming WebSocket connection (`/notebook` or `/manager`) runs as an independent `asyncio.Task`.
- **Concurrency Locking**: A global `asyncio.Lock` protects the shared dictionaries:
  - `self.notebooks`: Maps `notebook_id` to `NotebookConnection`.
  - `self.managers`: Maps `mgr_id` to the manager's WebSocket.
  - `self.pending`: Maps `correlation_id` to `(mgr_id, timestamp)`.
- **Monitor Loop**: A background task periodically checks runner heartbeats. If a runner's last heartbeat age exceeds `heartbeat_timeout` (default: 60s), its connection is closed and its status changed to `disconnected`.
- **Garbage Collection**: Stale correlation IDs in `self.pending` are cleaned up automatically after a 300-second TTL.

### 2.2. Remote Runner (`client.py`)
The runner coordinates several concurrent subsystems across threads to ensure it never stalls:

```
[Main Thread]
   │
   ├──> [Heartbeat Thread (daemon)]
   │       └─ Emits MessageType.HEARTBEAT every 10s
   │       └─ Increments consecutive_hb_misses; forces reconnect on 3 misses
   │
   ├──> [Command Queue Worker Thread]
   │       └─ Reads items from self._command_queue
   │       └─ Spawns a dedicated Thread per command:
   │             ├─ [Thread cmd-1] (e.g. long shell script)
   │             ├─ [Thread cmd-2] (e.g. pwd / telemetry)
   │             └─ [Thread cmd-N] ...
   │
   └──> [ManagedProcess Instances]
           ├─ [Pump Thread (stdout)] -> deques lines & notifies subscribers
           ├─ [Pump Thread (stderr)] -> deques lines & notifies subscribers
           └─ [Watch Thread]        -> proc.wait(), drains pumps, sets Event
```

#### Why Per-Command Threads?
In standard serial runners, if a command blocks (such as a shell script waiting for an interactive `[Y/n]` or SSH key prompt), the entire WebSocket receive loop stops. This causes:
1. Heartbeats to stop transmitting.
2. The proxy to declare the runner dead.
3. Subsequent diagnostic commands (e.g., `ps`, `pwd`, or `job-terminate`) to be impossible to execute.

In Remote-Cli, incoming messages are immediately placed in `self._command_queue` and dispatched to individual worker threads. If one shell command is blocked, you can immediately send `job-terminate` or `pwd` and have it execute without delay.

---

## 3. Resilience & Failure Recovery Mechanisms

### 3.1. Reconnection & Persistent Identity
When a runner starts:
1. It inspects `.notebook_identity.json`.
2. If an identity exists, it presents `{"notebook_id": "...", "secret": "...", "reconnect": true}` in its `REGISTER` message.
3. The proxy verifies the secret matches the existing `NotebookConnection`.
4. If valid, the new WebSocket immediately supersedes any stale connection without interrupting running background jobs.
5. If no identity exists, the proxy assigns a new unique ID (e.g. `nb-<10 hex digits>`) and generated secret, which the client writes to `.notebook_identity.json`.

### 3.2. Offline Command Queueing
If a manager sends a command while a notebook is temporarily disconnected (such as during kernel restarts or network blips):
1. The proxy detects `nb.connected == False`.
2. The proxy places the message into `nb.pending_queue` (up to `queue_max = 100`).
3. The proxy returns `MessageType.COMMAND_QUEUED` with queue depth to the manager.
4. When the runner reconnects, the proxy immediately flushes all queued messages in FIFO order.
5. The runner executes them and sends responses back to the originating manager via the correlated IDs.

*(**Durability Caveat**: The offline command queue resides entirely in proxy Python process memory. If the proxy server process itself is restarted, queued offline commands are not persisted to disk and will be lost. The queue is intended for transient network disconnects and kernel restarts).*

### 3.3. POSIX Process Group Supervision
When launching shell commands or background subprocesses:
- `subprocess.Popen` is invoked with `start_new_session=True` on POSIX systems.
- This creates a new process group with the child process as the process group leader (`pgid = pid`).
- When terminating (`_kill_process_tree`):
  1. `os.killpg(pgid, signal.SIGTERM)` terminates the entire process tree (all spawned forks and children).
  2. Waits up to 5 seconds.
  3. If any processes survive, escalates to `os.killpg(pgid, signal.SIGKILL)`.
  4. Waits up to 2 seconds for final cleanup.
This prevents zombie processes and orphaned GPU memory allocations.

### 3.4. Output Buffer Memory Safety
Each `ManagedProcess` stores output lines in bounded circular buffers (`collections.deque(maxlen=100000)`):
- Stores up to 100,000 lines of stdout and 100,000 lines of stderr.
- Line counters (`stdout_total`, `stderr_total`) track exact output volume even if older lines rotate out of memory.
- Old finished shell jobs (`job-*`) are automatically pruned when the count exceeds 100, preventing memory bloat during thousands of iterations.
- Explicit services started via `process_start` are never automatically pruned.

### 3.5. Streaming Disk I/O & Transfer Buffer Safety
- **Chunked File Streaming**: File transfers use 1 MB chunks read from and written to open file streams on disk rather than loading whole multi-gigabyte models into RAM.
- **SHA-256 Validation**: Downloads stream chunks into temporary files while computing a rolling SHA-256 digest, atomically replacing the destination path only after hash confirmation.
- **Buffer Garbage Collection**: In-flight upload chunks on the runner are cleaned up if idle for more than 10 minutes (600s), and chunk index 0 automatically resets any prior failed attempt.

### 3.6. Security & Execution Boundaries
- **Authentication**: Central proxy requires a shared secret token via query parameter (`?token=SECRET`). Runners negotiate a persistent UUID secret on registration to prevent session impersonation.
- **Privilege Model**: Commands execute under the user running `client.py` (typically `root` in Kaggle or Colab environments).
- **Filesystem Access**: The runner executes within the container's standard filesystem boundaries; callers have full access to read, write, and delete files accessible to the user process. Always deploy proxy authentication when exposing proxies over public tunnels.

---

## 4. Request-Response Message Lifecycle

```
Manager                         Proxy                          Runner
   │                              │                              │
   │  1. MessageType.COMMAND      │                              │
   │─────────────────────────────>│                              │
   │  (corr_id=UUID-A)            │                              │
   │                              │  2. Route to Runner WS       │
   │                              │─────────────────────────────>│
   │                              │                              │
   │                              │                              │ 3. Dispatch to worker
   │                              │                              │    Spawn process
   │                              │  4. MessageType.STREAM       │
   │  5. Stream to user           │<─────────────────────────────│
   │<─────────────────────────────│  (corr_id=UUID-A)            │
   │                              │                              │
   │                              │  6. COMMAND_RESPONSE         │
   │  7. Deliver to Manager       │<─────────────────────────────│
   │<─────────────────────────────│  (corr_id=UUID-A)            │
   │  (clean up pending entry)    │                              │
```

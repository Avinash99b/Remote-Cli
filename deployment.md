# Deployment — Testing Harness (local fleet tooling)

The live-debugging setup for talking to Kaggle notebooks in real time. Not the
production proxy — this is the **local-only fleet manager** used to push/shell/inspect
workers.

## Components

| File | Role |
|---|---|
| `testing_proxy.py` | Local fleet manager proxy (port 8765, `/manager` WS endpoint). Notebooks register here. |
| `inference_client_manager.py` | CLI: `list`, `push`, `pull`, `execute`, `shell`, `subprocess`, `logs`, `pwd`, `cd`, `mkdir`, `rm`, … |
| `kaggle_notebook_testing_runner.py` | Runs **inside the notebook**, connects back to the manager, spawns/ manages worker processes |
| `protocol.py` | Shared WS message / uuid helpers |
| `.notebook_identity.json` | Local notebook id + secret (gitignored) |

## Requirements

- Python 3.9+ (the checked-in `.venv` is 3.9; the dev box may rebuild with 3.10 + websockets 16)
- `websockets`, `httpx`, `requests` (install into the `.venv`)
- A public tunnel URL for the manager so notebooks can reach it (e.g. linkpulse/ngrok)

## Run

```bash
cd testing

# 1. Start the manager proxy (public URL for notebooks to reach)
.venv/bin/python testing_proxy.py --host 0.0.0.0 --port 8765 \
    --token optional-shared-secret [--log-level DEBUG]

# 2. From anywhere, list the fleet through the tunnel
.venv/bin/python inference_client_manager.py \
    --proxy-url wss://notebook-proxy.linkpulse.avinash9.in/manager list
```

## CLI usage

```bash
.venv/bin/python inference_client_manager.py \
  --proxy-url wss://notebook-proxy.linkpulse.avinash9.in/manager \
  list                                # registered notebooks
  push nb-<ID> /abs/local/path /kaggle/working/dest.py
  pull nb-<ID> /kaggle/working/src.py /abs/local/out.py
  shell nb-<ID> "cmd" --timeout 30     # --timeout AFTER the notebook id
  execute nb-<ID> --timeout 60 "python3 script.py"
  logs nb-<ID> --tail 100
  terminate nb-<ID> --all
```

## Env vars

| Env var | Used by | Notes |
|---|---|---|
| `TESTING_PROXY_URL` | manager CLI | Default `ws://localhost:8765/manager` |
| `TESTING_PROXY_TOKEN` | manager CLI | Optional shared token for `--token`-protected managers |

## Deploying a worker via the harness

```bash
# push the vLLM worker files onto the notebook
.venv/bin/python inference_client_manager.py --proxy-url wss://…/manager \
  push nb-<ID> /repo/Proxy-Client/vllm_inference_client.py /kaggle/working/vllm_inference_client.py
.venv/bin/python inference_client_manager.py --proxy-url wss://…/manager \
  push nb-<ID> /repo/Proxy-Client/vllm_inference_server.py /kaggle/working/vllm_inference_server.py

# start it (export the full tuned env in the SAME shell — see Proxy-Client/deployment.md)
.venv/bin/python inference_client_manager.py --proxy-url wss://…/manager \
  shell nb-<ID> "cd /kaggle/working && rm -f /tmp/worker_test.log && \
  export VLLM_MODEL_NAME='Qwen/Qwen2.5-7B-Instruct' VLLM_DTYPE=auto \
  TENSOR_PARALLEL_SIZE=2 MAX_MODEL_LEN=4096 GPU_MEMORY_UTILIZATION=0.85 \
  WORKER_ID='kaggle-account-1' WORKER_SECRET='change-me-worker-secret' \
  PROXY_URL='https://kaggle-proxy-server.linkpulse.avinash9.in' && \
  nohup python3 vllm_inference_client.py > /tmp/worker_test.log 2>&1 &" \
  --timeout 30
```

Then watch `/tmp/worker_test.log` (`logs nb-<ID> /tmp/worker_test.log --tail 100`).

## Gotchas

- `shell` right after `nohup … &` often times out ("no response within 30.0s") while the
  async task succeeds — re-read the log afterwards.
- `--timeout` is a flag of the `shell` subcommand and must come **after** the notebook id.
- A global `--timeout` before the subcommand is an "unrecognized arguments" error.
- `ManagedProcess.poll()` is broken (known bug) — use `shell` + `nohup` for long-running
  workers instead of `subprocess`.
- The manager may return HTTP 403 on a `shell` transiently — retry once.
#!/usr/bin/env python3
"""
kaggle_notebook_testing_runner.py
=================================

Remote agent that runs *inside an actual Kaggle notebook* and lets AI agents
drive the notebook through the testing proxy.

The runner connects outbound to testing_proxy.py, registers a stable
notebook id, then waits for commands. It exposes remote operations for:

  * file operations (upload/download/delete/move/copy, mkdir/rmdir, list)
  * shell execution (sync + async, with streaming)
  * process management (start/terminate/list/logs/wait/restart)
  * environment variables (set/unset/get/list, applied to future processes)
  * arbitrary Python execution inside a persistent notebook namespace
  * notebook system information (hostname, python, os, CUDA, GPUs, RAM,
    disk, cwd, uptime, installed packages)

It automatically reconnects after network interruptions, sends periodic
heartbeats, and reports its current workload and health.

Running inside Kaggle
---------------------
Pip-install the websockets package if needed, then launch this program in a
background cell (the notebook kernel keeps the loop alive):

    !pip install -q websockets
    !nohup python /kaggle/working/kaggle_notebook_testing_runner.py \\
        --proxy ws://<PROXY_HOST>:8765/notebook --token <OPTIONAL> \\
        > /kaggle/working/runner.log 2>&1 &

Or call :func:`start_in_background` from a Python cell:

    from kaggle_notebook_testing_runner import start_in_background
    start_in_background(proxy_url="ws://<PROXY_HOST>:8765/notebook",
                        token="<OPTIONAL>")

The runner persists its assigned notebook id + reconnect secret to
``.notebook_identity.json`` in its working directory so that reconnects keep
the same notebook id.

Usage
-----
    python kaggle_notebook_testing_runner.py \\
        --proxy ws://host:8765/notebook [--token SECRET] [--notebook-id ID]
"""

import argparse
import base64
import contextlib
import hashlib
import io
import json
import logging
import os
import platform
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import (  # noqa: E402
    Message, MessageType, CommandType, COMMAND_CHUNK_SIZE,
)

try:
    from websockets.sync.client import connect as _ws_connect
    _WS_SYNC = True
except ImportError:  # pragma: no cover - old websockets
    try:
        from websockets.sync.client import connect as _ws_connect  # type: ignore
        _WS_SYNC = True
    except ImportError:  # pragma: no cover
        _WS_SYNC = False

if not _WS_SYNC:  # pragma: no cover
    raise SystemExit(
        "This program requires the 'websockets' package (sync support).\n"
        "Install it with: pip install websockets"
    )

log = logging.getLogger("kaggle_runner")


class ManagedProcess:
    """A long-running subprocess with captured output buffers."""

    def __init__(self, process_id: str, command: str, proc: subprocess.Popen,
                 cwd: str, env: Dict[str, str], logger, restart_count: int = 0) -> None:
        self.process_id = process_id
        self.command = command
        self.proc = proc
        self.cwd = cwd
        self.env = dict(env)
        self.logger = logger
        self.status = "running"
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.exit_code: Optional[int] = None
        self.restart_count = restart_count
        self.stdout_lines: List[str] = []
        self.stderr_lines: List[str] = []
        self._lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        if proc.stdout is not None:
            self._start_pump(proc.stdout, self.stdout_lines, "stdout")
        if proc.stderr is not None:
            self._start_pump(proc.stderr, self.stderr_lines, "stderr")

    def _start_pump(self, stream, buf: List[str], tag: str) -> None:
        def _pump():
            try:
                for raw in iter(stream.readline, b""):
                    line = raw.decode(errors="replace").rstrip("\n")
                    with self._lock:
                        buf.append(line)
                stream.close()
            except Exception as exc:  # pragma: no cover
                self.logger.debug("pump %s error: %s", tag, exc)
        t = threading.Thread(target=_pump, daemon=True)
        t.start()
        self._threads.append(t)

    def check(self) -> None:
        if self.status == "running":
            rc = self.proc.poll()
            if rc is not None:
                self.status = "completed" if rc == 0 else "failed"
                self.exit_code = rc
                self.end_time = time.time()

    def info(self) -> Dict[str, Any]:
        self.check()
        return {
            "process_id": self.process_id,
            "command": self.command,
            "status": self.status,
            "pid": self.proc.pid,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "exit_code": self.exit_code,
            "runtime_seconds": round(
                (self.end_time or time.time()) - self.start_time, 2),
            "stdout_lines": len(self.stdout_lines),
            "stderr_lines": len(self.stderr_lines),
            "cwd": self.cwd,
            "restart_count": self.restart_count,
        }


import threading


class KaggleNotebookRunner:
    """Main runner: connects to the proxy and serves remote commands."""

    def __init__(self, proxy_url: str, token: Optional[str] = None,
                 notebook_id: Optional[str] = None,
                 identity_file: str = ".notebook_identity.json",
                 heartbeat_interval: float = 10.0,
                 reconnect_max_delay: float = 30.0) -> None:
        self.proxy_url = proxy_url
        self.token = token
        self.requested_id = notebook_id
        self.identity_file = identity_file
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_max_delay = reconnect_max_delay

        self.running = True
        self.connected = False
        self.notebook_id: Optional[str] = None
        self.secret: Optional[str] = None
        self.ws = None
        self.current_command: Optional[Dict[str, Any]] = None
        self.started_at = time.time()
        self.command_count = 0

        self._cwd = os.getcwd()
        self._ns: Dict[str, Any] = {
            "__name__": "__remote_testing__",
            "__builtins__": __builtins__,
        }
        self.processes: Dict[str, ManagedProcess] = {}
        self._upload_buffers: Dict[str, Dict[str, Any]] = {}
        self._proc_lock = threading.Lock()

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartstop = threading.Event()

    # ------------------------------------------------------------------ #
    # Public control
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        try:
            self._run_loop()
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self, terminate_processes: bool = False) -> None:
        if not self.running:
            return
        log.info("runner shutting down")
        self.running = False
        self._heartstop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
        if terminate_processes:
            for p in list(self.processes.values()):
                try:
                    p.proc.terminate()
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Synchronous main loop with auto-reconnect + heartbeats
    # ------------------------------------------------------------------ #
    def _run_loop(self) -> None:
        delay = 1.0
        while self.running:
            try:
                self._connect_once()
                delay = 1.0
            except Exception as exc:
                log.warning("connection error: %s", exc)
            if self.running:
                time.sleep(delay)
                delay = min(delay * 2, self.reconnect_max_delay)
        log.info("runner loop ended")

    def _connect_once(self) -> None:
        url = self.proxy_url
        if self.token:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}token={self.token}"

        kwargs = dict(max_size=None, ping_interval=20, ping_timeout=20)
        ws = _ws_connect(url, **kwargs)
        self.ws = ws
        self.connected = True
        log.info("connected to proxy %s", url)

        reg = Message(
            message_type=MessageType.REGISTER,
            payload={
                "notebook_id": self.notebook_id or self.requested_id or "auto",
                "secret": self.secret or "",
                "info": self.system_info(),
                "reconnect": self.notebook_id is not None,
            },
        )
        ws.send(reg.to_json())
        raw = ws.recv()
        ack = Message.from_json(raw)
        if ack.message_type != MessageType.REGISTER_ACK:
            log.error("register failed: %s", ack.payload)
            ws.close()
            return

        assigned = ack.payload.get("notebook_id")
        self.notebook_id = assigned
        if ack.payload.get("secret"):
            self.secret = ack.payload["secret"]
        self._save_identity()
        if ack.payload.get("queued_flushed"):
            log.info("resumed notebook %s; flushed %s queued commands",
                     assigned, ack.payload["queued_flushed"])
        else:
            log.info("registered as notebook %s", assigned)

        # Start heartbeat loop in a daemon thread
        self._heartstop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, args=(ws,), daemon=True)
        self._heartbeat_thread.start()

        try:
            while self.running:
                try:
                    raw = ws.recv()
                except Exception:
                    break
                try:
                    msg = Message.from_json(raw)
                except json.JSONDecodeError:
                    continue
                if msg.message_type == MessageType.COMMAND:
                    self._handle_command(msg)
        finally:
            self._heartstop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=5)
            self.connected = False
            self.ws = None
            log.info("disconnected from proxy")

    def _heartbeat_loop(self, ws) -> None:
        while not self._heartstop.wait(timeout=self.heartbeat_interval):
            if not self.running or not self.connected:
                return
            try:
                ws.send(Message(
                    message_type=MessageType.HEARTBEAT,
                    payload={
                        "workload": self.workload(),
                        "info": self.system_info(),
                        "uptime": time.time() - self.started_at,
                    },
                ).to_json())
            except Exception:
                return

    def workload(self) -> Dict[str, Any]:
        running = 0
        with self._proc_lock:
            for p in self.processes.values():
                p.check()
                if p.status == "running":
                    running += 1
        return {
            "running_processes": running,
            "total_processes": len(self.processes),
            "current_command": self.current_command,
            "commands_served": self.command_count,
        }

    # ------------------------------------------------------------------ #
    # Command dispatch
    # ------------------------------------------------------------------ #
    def _handle_command(self, msg: Message) -> None:
        payload = msg.payload or {}
        command = payload.get("command")
        self.command_count += 1
        self.current_command = {"command": command, "message_id": msg.message_id}
        log.info("command %s (%s)", command, msg.message_id[:8])
        try:
            handler = getattr(self, f"_cmd_{command}", None)
            if handler is None:
                self._reply(msg, {
                    "success": False,
                    "error": f"unknown command {command!r}",
                })
                return
            result = handler(msg)
            if isinstance(result, dict) and result.get("success") is False:
                pass  # synchronous return
            self._reply(msg, result)
        except Exception as exc:
            log.exception("command %s failed", command)
            self._reply(msg, {
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
            })
        finally:
            self.current_command = None

    def _reply(self, msg: Message, payload: Dict[str, Any]) -> None:
        if not self.connected or self.ws is None:
            return
        try:
            self.ws.send(Message.create_response(msg, payload).to_json())
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Environment helpers
    # ------------------------------------------------------------------ #
    def _merged_env(self, extra: Optional[Dict[str, str]]) -> Dict[str, str]:
        env = dict(os.environ)
        env.update(self._env_overlay())
        if extra:
            env.update({k: str(v) for k, v in extra.items()})
        return env

    def _env_overlay(self) -> Dict[str, str]:
        return self._ns.get("__env_overlay__", {})

    @staticmethod
    def _resolve(path: str, base: str) -> str:
        if os.path.isabs(path):
            return path
        return os.path.join(base, path)

    # ------------------------------------------------------------------ #
    # File operations
    # ------------------------------------------------------------------ #
    def _cmd_file_upload(self, msg: Message) -> Optional[Dict[str, Any]]:
        payload = msg.payload
        remote_path = payload["remote_path"]
        target = self._resolve(remote_path, self._cwd)
        os.makedirs(os.path.dirname(target) or ".", exist_ok=True)

        encoding = payload.get("encoding", "base64")
        content = payload["content"]
        if encoding == "base64":
            data = base64.b64decode(content)
        elif encoding == "text":
            data = content.encode()
        else:
            raise ValueError(f"unsupported encoding {encoding!r}")

        chunk_index = int(payload.get("chunk_index", 0))
        total_chunks = int(payload.get("total_chunks", 1))

        if total_chunks <= 1:
            with open(target, "wb") as fh:
                fh.write(data)
            return {
                "success": True, "complete": True,
                "path": target, "bytes": len(data),
            }

        buf = self._upload_buffers.setdefault(
            target, {"chunks": {}, "total": total_chunks})
        buf["chunks"][chunk_index] = data
        if len(buf["chunks"]) >= buf["total"]:
            ordered = b"".join(buf["chunks"][i] for i in range(buf["total"]))
            self._upload_buffers.pop(target, None)
            with open(target, "wb") as fh:
                fh.write(ordered)
            return {
                "success": True, "complete": True,
                "path": target, "bytes": len(ordered),
            }
        return {
            "success": True, "complete": False,
            "chunk_index": chunk_index,
            "received": len(buf["chunks"]), "total": total_chunks,
        }

    def _cmd_file_download(self, msg: Message) -> None:
        payload = msg.payload
        remote_path = payload["remote_path"]
        target = self._resolve(remote_path, self._cwd)
        if not os.path.isfile(target):
            self._reply(msg, {
                "success": False, "error": f"no such file: {target}",
            })
            return
        with open(target, "rb") as fh:
            data = fh.read()
        total = max(1, (len(data) + COMMAND_CHUNK_SIZE - 1) // COMMAND_CHUNK_SIZE)
        for i in range(total):
            chunk = data[i * COMMAND_CHUNK_SIZE:(i + 1) * COMMAND_CHUNK_SIZE]
            self._stream(msg, {
                "kind": "download_chunk",
                "path": target,
                "chunk_index": i,
                "total_chunks": total,
                "content": base64.b64encode(chunk).decode(),
                "encoding": "base64",
                "complete": False,
            })
        self._reply(msg, {
            "kind": "download_complete",
            "path": target,
            "bytes": len(data),
            "total_chunks": total,
            "complete": True,
            "sha256": hashlib.sha256(data).hexdigest(),
        })

    def _cmd_file_delete(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["remote_path"], self._cwd)
        recursive = msg.payload.get("recursive", False)
        if not os.path.exists(path):
            return {"success": False, "error": f"no such path: {path}"}
        if os.path.isdir(path) and not recursive:
            shutil.rmtree(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        return {"success": True, "path": path}

    def _cmd_file_move(self, msg: Message) -> Dict[str, Any]:
        src = self._resolve(msg.payload["src"], self._cwd)
        dst = self._resolve(msg.payload["dst"], self._cwd)
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        shutil.move(src, dst)
        return {"success": True, "src": src, "dst": dst}

    def _cmd_file_copy(self, msg: Message) -> Dict[str, Any]:
        src = self._resolve(msg.payload["src"], self._cwd)
        dst = self._resolve(msg.payload["dst"], self._cwd)
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
        return {"success": True, "src": src, "dst": dst}

    def _cmd_file_mkdir(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["path"], self._cwd)
        os.makedirs(path, exist_ok=msg.payload.get("exist_ok", True))
        return {"success": True, "path": path}

    def _cmd_file_rmdir(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["path"], self._cwd)
        recursive = msg.payload.get("recursive", False)
        if recursive:
            shutil.rmtree(path)
        else:
            os.rmdir(path)
        return {"success": True, "path": path}

    def _cmd_file_list(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload.get("path", "."), self._cwd)
        recursive = msg.payload.get("recursive", False)
        if not os.path.exists(path):
            return {"success": False, "error": f"no such path: {path}"}
        entries = []
        if recursive:
            for root, dirs, files in os.walk(path):
                for name in files:
                    fp = os.path.join(root, name)
                    entries.append(self._file_info(fp))
                for name in dirs:
                    fp = os.path.join(root, name)
                    entries.append(self._file_info(fp))
        else:
            for name in sorted(os.listdir(path)):
                entries.append(self._file_info(os.path.join(path, name)))
        return {"success": True, "path": path, "recursive": recursive,
                "files": entries}

    def _cmd_file_stat(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["path"], self._cwd)
        if not os.path.exists(path):
            return {"success": False, "error": f"no such path: {path}"}
        return {"success": True, "file": self._file_info(path)}

    def _cmd_file_exists(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["path"], self._cwd)
        return {"success": True, "path": path, "exists": os.path.exists(path),
                "is_dir": os.path.isdir(path)}

    @staticmethod
    def _file_info(path: str) -> Dict[str, Any]:
        st = os.stat(path)
        return {
            "name": os.path.basename(path),
            "path": path,
            "is_dir": os.path.isdir(path),
            "size": st.st_size,
            "modified": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime(st.st_mtime)),
            "permissions": oct(st.st_mode & 0o777),
        }

    # ------------------------------------------------------------------ #
    # Shell execution
    # ------------------------------------------------------------------ #
    def _cmd_shell_exec(self, msg: Message) -> Dict[str, Any]:
        payload = msg.payload
        cmd = payload["shell_command"]
        cwd = self._resolve(payload.get("cwd") or ".", self._cwd)
        env = self._merged_env(payload.get("env"))
        timeout = payload.get("timeout")
        stream = payload.get("stream", False)

        if stream:
            return self._shell_stream(cmd, cwd, env, timeout, msg)

        t0 = time.time()
        try:
            completed = subprocess.run(
                cmd, shell=True, cwd=cwd, env=env,
                capture_output=True, text=True, timeout=timeout)
            return {
                "success": completed.returncode == 0,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
                "exit_code": completed.returncode,
                "execution_time": round(time.time() - t0, 3),
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "success": False,
                "stdout": (exc.stdout or ""),
                "stderr": (exc.stderr or "") + "\n[timed out after "
                          f"{timeout}s]",
                "exit_code": -1,
                "execution_time": round(time.time() - t0, 3),
                "error": f"timed out after {timeout}s",
            }
        except Exception as exc:
            return {"success": False, "error": f"{type(exc).__name__}: {exc}",
                    "exit_code": -1,
                    "execution_time": round(time.time() - t0, 3)}

    def _shell_stream(self, cmd, cwd, env, timeout, msg) -> Dict[str, Any]:
        t0 = time.time()
        try:
            proc = subprocess.Popen(
                cmd, cwd=cwd, env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = [], []

            def pump(stream, kind, buf):
                while True:
                    raw = stream.readline()
                    if not raw:
                        break
                    line = raw.decode(errors="replace").rstrip("\n")
                    buf.append(line)
                    self._stream(msg, {
                        "kind": "shell_line", "stream": kind, "line": line,
                    })

            import select
            deadline = time.time() + timeout if timeout else None
            while True:
                if timeout and time.time() > deadline:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()
                    return {
                        "success": False, "error": f"timed out after {timeout}s",
                        "exit_code": -1, "execution_time": round(time.time() - t0, 3),
                    }
                r, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.1)
                if proc.stdout in r:
                    raw = proc.stdout.readline()
                    if not raw:
                        pass
                    else:
                        line = raw.decode(errors="replace").rstrip("\n")
                        out.append(line)
                        self._stream(msg, {
                            "kind": "shell_line", "stream": "stdout", "line": line,
                        })
                if proc.stderr in r:
                    raw = proc.stderr.readline()
                    if not raw:
                        pass
                    else:
                        line = raw.decode(errors="replace").rstrip("\n")
                        err.append(line)
                        self._stream(msg, {
                            "kind": "shell_line", "stream": "stderr", "line": line,
                        })
                if proc.poll() is not None:
                    break

            # Read remaining output
            for stream, buf in [(proc.stdout, out), (proc.stderr, err)]:
                while True:
                    raw = stream.readline()
                    if not raw:
                        break
                    line = raw.decode(errors="replace").rstrip("\n")
                    buf.append(line)

            rc = proc.wait()
            return {
                "success": rc == 0,
                "stdout": "\n".join(out),
                "stderr": "\n".join(err),
                "exit_code": rc,
                "execution_time": round(time.time() - t0, 3),
                "streamed": True,
            }
        except Exception as exc:
            try:
                proc.terminate()
            except Exception:
                pass
            return {
                "success": False, "error": f"{type(exc).__name__}: {exc}",
                "exit_code": -1, "execution_time": round(time.time() - t0, 3),
            }

    # ------------------------------------------------------------------ #
    # Process management
    # ------------------------------------------------------------------ #
    def _cmd_process_start(self, msg: Message) -> Dict[str, Any]:
        payload = msg.payload
        cmd = payload["shell_command"]
        process_id = payload.get("process_id") or uuid.uuid4().hex[:12]
        cwd = self._resolve(payload.get("cwd") or ".", self._cwd)
        env = self._merged_env(payload.get("env"))

        proc = subprocess.Popen(
            cmd, cwd=cwd, env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        managed = ManagedProcess(process_id, cmd, proc, cwd, env, log)
        with self._proc_lock:
            self.processes[process_id] = managed
        return {"success": True, "process": managed.info()}

    def _cmd_process_restart(self, msg: Message) -> Dict[str, Any]:
        process_id = msg.payload["process_id"]
        with self._proc_lock:
            existing = self.processes.get(process_id)
            if existing is None:
                return {"success": False,
                        "error": f"unknown process_id {process_id!r}"}
            old = existing
            del self.processes[process_id]
        try:
            old.proc.terminate()
        except Exception:
            pass
        try:
            old.proc.wait(timeout=5)
        except Exception:
            pass

        proc = subprocess.Popen(
            old.command, cwd=old.cwd, env=old.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        managed = ManagedProcess(process_id, old.command, proc, old.cwd,
                                 old.env, log, restart_count=old.restart_count + 1)
        with self._proc_lock:
            self.processes[process_id] = managed
        return {"success": True, "process": managed.info()}

    def _cmd_process_terminate(self, msg: Message) -> Dict[str, Any]:
        process_id = msg.payload["process_id"]
        sig = msg.payload.get("signal", 15)
        with self._proc_lock:
            mp = self.processes.get(process_id)
        if mp is None:
            return {"success": False, "error": f"unknown process_id {process_id!r}"}
        try:
            if sig == 15:
                mp.proc.terminate()
            else:
                import os
                os.kill(mp.proc.pid, sig)
            mp.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            mp.proc.kill()
        except Exception:
            pass
        mp.check()
        return {"success": True, "process": mp.info()}

    def _cmd_process_list(self, msg: Message) -> Dict[str, Any]:
        infos = []
        with self._proc_lock:
            for p in self.processes.values():
                p.check()
                infos.append(p.info())
        return {"success": True, "processes": infos}

    def _cmd_process_logs(self, msg: Message) -> Dict[str, Any]:
        process_id = msg.payload["process_id"]
        tail = int(msg.payload.get("tail", 100))
        with self._proc_lock:
            mp = self.processes.get(process_id)
        if mp is None:
            return {"success": False, "error": f"unknown process_id {process_id!r}"}
        with mp._lock:
            out = mp.stdout_lines[-tail:]
            err = mp.stderr_lines[-tail:]
        return {
            "success": True,
            "process_id": process_id,
            "stdout": "\n".join(out),
            "stderr": "\n".join(err),
            "stdout_tail": len(out),
            "stderr_tail": len(err),
        }

    def _cmd_process_wait(self, msg: Message) -> Dict[str, Any]:
        process_id = msg.payload["process_id"]
        timeout = msg.payload.get("timeout")
        with self._proc_lock:
            mp = self.processes.get(process_id)
        if mp is None:
            return {"success": False, "error": f"unknown process_id {process_id!r}"}
        try:
            if timeout:
                try:
                    import select
                    select.select([], [], [], timeout)
                except Exception:
                    pass
            else:
                mp.proc.wait()
        except Exception:
            pass
        mp.check()
        return {"success": True, "process": mp.info()}

    # ------------------------------------------------------------------ #
    # Environment variables
    # ------------------------------------------------------------------ #
    def _cmd_env_list(self, msg: Message) -> Dict[str, Any]:
        overlay = self._env_overlay()
        env = dict(os.environ)
        env.update(overlay)
        return {"success": True, "environment": env, "overlay": overlay}

    def _cmd_env_get(self, msg: Message) -> Dict[str, Any]:
        key = msg.payload["key"]
        return {"success": True, "key": key,
                "value": self._merged_env(None).get(key)}

    def _cmd_env_set(self, msg: Message) -> Dict[str, Any]:
        key = msg.payload["key"]
        value = msg.payload["value"]
        os.environ[key] = str(value)
        overlay = self._ns.setdefault("__env_overlay__", {})
        overlay[key] = str(value)
        return {"success": True, "key": key, "value": str(value)}

    def _cmd_env_unset(self, msg: Message) -> Dict[str, Any]:
        key = msg.payload["key"]
        os.environ.pop(key, None)
        overlay = self._ns.setdefault("__env_overlay__", {})
        overlay.pop(key, None)
        return {"success": True, "key": key}

    # ------------------------------------------------------------------ #
    # Python execution (persistent namespace)
    # ------------------------------------------------------------------ #
    def _cmd_python_exec(self, msg: Message) -> Dict[str, Any]:
        code = msg.payload["code"]
        capture_locals = msg.payload.get("capture_locals", False)

        buf_out, buf_err = io.StringIO(), io.StringIO()
        result = None
        exc = None
        try:
            with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
                try:
                    result = eval(compile(code, "<remote-exec>", "eval"), self._ns)
                except SyntaxError:
                    exec(compile(code, "<remote-exec>", "exec"), self._ns)
                    result = self._ns.get("__result__")
        except Exception as e:
            exc = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        return {
            "success": exc is None,
            "stdout": buf_out.getvalue(),
            "stderr": buf_err.getvalue(),
            "result": result if capture_locals else self._stringify(result),
            "exception": exc,
            "persistent_namespace_keys": sorted(self._ns.keys())[:50],
        }

    @staticmethod
    def _stringify(value: Any) -> Any:
        if value is None or isinstance(value, (int, float, bool, str)):
            return value
        try:
            return json.loads(json.dumps(value, default=str))
        except Exception:
            return str(value)

    # ------------------------------------------------------------------ #
    # cwd operations
    # ------------------------------------------------------------------ #
    def _cmd_pwd(self, msg: Message) -> Dict[str, Any]:
        return {"success": True, "cwd": self._cwd}

    def _cmd_cd(self, msg: Message) -> Dict[str, Any]:
        path = self._resolve(msg.payload["path"], self._cwd)
        if not os.path.isdir(path):
            return {"success": False, "error": f"not a directory: {path}"}
        self._cwd = os.path.realpath(path)
        os.chdir(self._cwd)
        return {"success": True, "cwd": self._cwd}

    # ------------------------------------------------------------------ #
    # Notebook control
    # ------------------------------------------------------------------ #
    def _cmd_notebook_ping(self, msg: Message) -> Dict[str, Any]:
        return {"success": True, "status": "pong",
                "notebook_id": self.notebook_id,
                "timestamp": time.time(),
                "uptime": round(time.time() - self.started_at, 2)}

    def _cmd_notebook_shutdown(self, msg: Message) -> Dict[str, Any]:
        terminate = msg.payload.get("terminate_processes", True)
        if self.ws is not None:
            try:
                self._reply(msg, {"success": True,
                                  "shutdown": True})
            except Exception:
                pass
        self.shutdown(terminate_processes=terminate)
        return None

    # ------------------------------------------------------------------ #
    # System information
    # ------------------------------------------------------------------ #
    def _cmd_system_info(self, msg: Message) -> Dict[str, Any]:
        info = self.system_info()
        info["success"] = True
        return info

    def system_info(self) -> Dict[str, Any]:
        ram = self._ram_info()
        disk = self._disk_info()
        gpus = self._gpu_info()
        return {
            "hostname": socket.gethostname(),
            "python_version": platform.python_version(),
            "python_executable": sys.executable,
            "os": platform.platform(),
            "os_release": platform.release(),
            "arch": platform.machine(),
            "cpu_count": os.cpu_count(),
            "uptime_seconds": self._uptime(),
            "cwd": self._cwd,
            "process_pid": os.getpid(),
            "ram": ram,
            "disk": disk,
            "cuda_version": self._cuda_version(),
            "gpus": gpus,
            "installed_packages": self._installed_packages(),
        }

    @staticmethod
    def _uptime() -> float:
        try:
            with open("/proc/uptime") as fh:
                return float(fh.read().split()[0])
        except Exception:
            return time.time() - time.monotonic()

    @staticmethod
    def _ram_info() -> Dict[str, Any]:
        try:
            with open("/proc/meminfo") as fh:
                fields = {}
                for line in fh:
                    parts = line.split(":")
                    if len(parts) == 2:
                        fields[parts[0]] = int(parts[1].strip().split()[0]) * 1024
            total = fields.get("MemTotal", 0)
            available = fields.get("MemAvailable", fields.get("MemFree", 0))
            used = total - available
            return {
                "total_bytes": total,
                "available_bytes": available,
                "used_bytes": used,
                "percent_used": round(100.0 * used / total, 2) if total else None,
            }
        except Exception:
            return {"total_bytes": None, "available_bytes": None,
                    "used_bytes": None, "percent_used": None}

    @staticmethod
    def _disk_info() -> List[Dict[str, Any]]:
        mounts = []
        try:
            with open("/proc/mounts") as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].startswith("/"):
                        mounts.append(parts[1])
        except Exception:
            mounts = ["/"]
        seen, out = set(), []
        for mount in mounts:
            if mount in seen:
                continue
            seen.add(mount)
            try:
                usage = shutil.disk_usage(mount)
                out.append({
                    "mount": mount,
                    "total_bytes": usage.total,
                    "used_bytes": usage.used,
                    "free_bytes": usage.free,
                })
            except Exception:
                continue
        return out

    @staticmethod
    def _gpu_info() -> List[Dict[str, Any]]:
        cmd = [
            "nvidia-smi",
            "--query-gpu=index,name,memory.total,memory.free,memory.used,"
            "temperature.gpu,utilization.gpu",
            "--format=csv,noheader,nounits",
        ]
        try:
            out = subprocess.run(cmd, capture_output=True, text=True,
                                 timeout=20).stdout.strip()
        except Exception:
            return []
        gpus = []
        for line in out.splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 7:
                continue
            try:
                gpus.append({
                    "index": int(parts[0]),
                    "name": parts[1],
                    "memory_total_mb": int(parts[2]),
                    "memory_free_mb": int(parts[3]),
                    "memory_used_mb": int(parts[4]),
                    "temperature_c": int(parts[5]),
                    "utilization_percent": int(parts[6]),
                })
            except ValueError:
                continue
        return gpus

    @staticmethod
    def _cuda_version() -> Optional[str]:
        try:
            out = subprocess.run(["nvidia-smi"], capture_output=True,
                                 text=True, timeout=20).stdout
            for line in out.splitlines():
                if "CUDA Version" in line:
                    return line.split("CUDA Version:")[-1].strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _installed_packages() -> List[str]:
        try:
            out = subprocess.run(
                [sys.executable, "-m", "pip", "list", "--format=json"],
                capture_output=True, text=True, timeout=30,
            ).stdout
            packages = json.loads(out)
            return [f"{p['name']}=={p['version']}" for p in packages]
        except Exception:
            return []

    # ------------------------------------------------------------------ #
    # Identity persistence
    # ------------------------------------------------------------------ #
    def _save_identity(self) -> None:
        if not self.notebook_id:
            return
        path = self._resolve(self.identity_file, self._cwd)
        try:
            with open(path, "w") as fh:
                json.dump({"notebook_id": self.notebook_id,
                           "secret": self.secret}, fh)
        except Exception as exc:
            log.warning("could not save identity to %s: %s", path, exc)

    def _load_identity(self) -> None:
        path = self._resolve(self.identity_file, self._cwd)
        try:
            with open(path) as fh:
                data = json.load(fh)
            self.notebook_id = data.get("notebook_id")
            self.secret = data.get("secret")
            log.info("loaded identity %s from %s", self.notebook_id, path)
        except Exception:
            pass


def start_in_background(proxy_url: str, token: Optional[str] = None,
                        notebook_id: Optional[str] = None,
                        heartbeat_interval: float = 10.0) -> threading.Thread:
    """Start the runner in a background daemon thread (for notebook cells)."""
    runner = KaggleNotebookRunner(
        proxy_url=proxy_url, token=token, notebook_id=notebook_id,
        heartbeat_interval=heartbeat_interval,
    )
    runner._load_identity()

    def _run():
        try:
            runner.run()
        except Exception:
            log.exception("background runner exited")

    thread = threading.Thread(target=_run, daemon=True, name="kaggle-runner")
    thread.start()
    return thread


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remote testing agent for a Kaggle notebook")
    parser.add_argument(
        "--proxy", required=True,
        help="proxy URL, e.g. ws://host:8765/notebook")
    parser.add_argument("--token", default=None)
    parser.add_argument("--notebook-id", default=None)
    parser.add_argument("--identity-file", default=".notebook_identity.json")
    parser.add_argument("--heartbeat-interval", type=float, default=10.0)
    parser.add_argument("--reconnect-max-delay", type=float, default=30.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    runner = KaggleNotebookRunner(
        proxy_url=args.proxy, token=args.token, notebook_id=args.notebook_id,
        identity_file=args.identity_file,
        heartbeat_interval=args.heartbeat_interval,
        reconnect_max_delay=args.reconnect_max_delay,
    )
    runner._load_identity()

    def _signal(signum, frame):
        runner.shutdown()

    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    runner.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
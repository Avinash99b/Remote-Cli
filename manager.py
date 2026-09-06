#!/usr/bin/env python3
"""
inference_client_manager.py
===========================
Command-line controller for AI agents (and humans) that want to verify the
inference-proxy infrastructure against real Kaggle notebook runners.

This program talks *only* to testing_proxy.py. It can list connected
notebooks, push/pull files, run shell commands, manage subprocesses, inspect
or mutate environment variables, execute Python snippets, and query live
system information -- all routed through the proxy to a targeted notebook.

Output is JSON by default (``--pretty`` for indented, human-friendly output)
so both humans and agents can parse it. Exit code is 0 on success and 1 on
remote/proxy error.

Usage
-----
    python inference_client_manager.py [--proxy-url ws://host:8765/manager] \\
        [--token SECRET] <command> <args...>

Example commands
----------------
    python inference_client_manager.py list
    python inference_client_manager.py info nb-abc123
    python inference_client_manager.py push nb-abc123 proxy.py
    python inference_client_manager.py pull nb-abc123 remote.log
    python inference_client_manager.py execute nb-abc123 my_test.py
    python inference_client_manager.py shell nb-abc123 "pip install vllm"
    python inference_client_manager.py subprocess nb-abc123 "python server.py"
    python inference_client_manager.py terminate nb-abc123 <process_id>
    python inference_client_manager.py ps nb-abc123
    python inference_client_manager.py logs nb-abc123 [process_id]
    python inference_client_manager.py pwd nb-abc123
    python inference_client_manager.py cd nb-abc123 /kaggle/working
    python inference_client_manager.py mkdir nb-abc123 models
    python inference_client_manager.py rm nb-abc123 server.py
    python inference_client_manager.py env list nb-abc123
    python inference_client_manager.py env get nb-abc123 HF_TOKEN
    python inference_client_manager.py env set nb-abc123 HF_TOKEN xxx
    python inference_client_manager.py env unset nb-abc123 HF_TOKEN
    python inference_client_manager.py heartbeat
    python inference_client_manager.py ping nb-abc123
    python inference_client_manager.py system-info nb-abc123
    python inference_client_manager.py reconnect

Configuration can also be supplied via environment variables
``TESTING_PROXY_URL`` and ``TESTING_PROXY_TOKEN``.
"""

import argparse
import asyncio
import base64
import hashlib
import json
import os
import shlex
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import Message, MessageType, COMMAND_CHUNK_SIZE  # noqa: E402

try:
    from websockets.asyncio.client import connect as _ws_connect
    _WS_NEW_API = True
except ImportError:  # pragma: no cover - old websockets (<12)
    try:
        from websockets import connect as _ws_connect  # type: ignore
        _WS_NEW_API = False
    except ImportError:  # pragma: no cover
        _ws_connect = None

if _ws_connect is None:  # pragma: no cover
    raise SystemExit(
        "This program requires the 'websockets' package.\n"
        "Install it with: pip install websockets"
    )


class TimeoutError(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, result: Dict[str, Any]) -> None:
        super().__init__(result.get("error", "unknown remote error"))
        self.result = result


class InferenceClientManager:
    """Client that issues commands to the proxy on behalf of the agent."""

    def __init__(self, proxy_url: str, token: Optional[str] = None,
                 timeout: Optional[float] = 120.0) -> None:
        self.proxy_url = proxy_url
        self.token = token
        self.timeout = timeout if timeout is not None else 120.0

    def _url(self) -> str:
        if not self.token:
            return self.proxy_url
        sep = "&" if "?" in self.proxy_url else "?"
        return f"{self.proxy_url}{sep}token={self.token}"

    async def _open(self):
        return await _ws_connect(
            self._url(), max_size=None, ping_interval=20, ping_timeout=20)

    # ------------------------------------------------------------------ #
    # Low-level request/response
    # ------------------------------------------------------------------ #
    async def request(self, command: str, notebook_id: Optional[str] = None,
                      extra: Optional[Dict[str, Any]] = None,
                      timeout: Optional[float] = None,
                      stream_cb: Optional[Callable[[Dict[str, Any]], None]] = None
                      ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"command": command}
        if notebook_id:
            payload["notebook_id"] = notebook_id
        if extra:
            payload.update(extra)

        msg = Message(message_type=MessageType.COMMAND, payload=payload)

        async with await self._open() as ws:
            await ws.send(msg.to_json())
            return await self._wait_response(
                ws, msg.message_id, timeout, stream_cb)

    async def _wait_response(self, ws, corr_id: str,
                             timeout: Optional[float],
                             stream_cb: Optional[Callable[[Dict[str, Any]], None]]
                             ) -> Dict[str, Any]:
        eff_timeout = timeout if timeout is not None else self.timeout
        if eff_timeout is None:
            eff_timeout = 120.0
        deadline = time.time() + eff_timeout
        streams: List[Dict[str, Any]] = []
        queued: Optional[Dict[str, Any]] = None
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"no response within {eff_timeout}s")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"no response within {eff_timeout}s")
            rmsg = Message.from_json(raw)
            if rmsg.correlation_id != corr_id:
                continue
            if rmsg.message_type == MessageType.COMMAND_STREAM:
                streams.append(rmsg.payload)
                if stream_cb:
                    stream_cb(rmsg.payload)
                continue
            if rmsg.message_type == MessageType.COMMAND_QUEUED:
                queued = rmsg.payload
                continue
            if rmsg.message_type == MessageType.ERROR:
                return {"success": False, "error": str(rmsg.payload),
                        "streamed": streams}
            if rmsg.message_type == MessageType.COMMAND_RESPONSE:
                result = dict(rmsg.payload or {})
                if streams:
                    result["streamed"] = streams
                if queued:
                    result["queued"] = queued
                return result
            # ignore any other correlated message types

    # ------------------------------------------------------------------ #
    # File helpers (chunked transfer on a single connection)
    # ------------------------------------------------------------------ #
    async def push(self, notebook_id: str, local_path: str,
                   remote_path: Optional[str] = None) -> Dict[str, Any]:
        if not os.path.isfile(local_path):
            raise FileNotFoundError(local_path)
        with open(local_path, "rb") as fh:
            data = fh.read()
        remote = remote_path or os.path.basename(local_path)

        total = max(1, (len(data) + COMMAND_CHUNK_SIZE - 1) // COMMAND_CHUNK_SIZE)
        last: Optional[Dict[str, Any]] = None

        async with await self._open() as ws:
            for i in range(total):
                chunk = data[i * COMMAND_CHUNK_SIZE:(i + 1) * COMMAND_CHUNK_SIZE]
                payload = {
                    "command": "file_upload",
                    "notebook_id": notebook_id,
                    "remote_path": remote,
                    "encoding": "base64",
                    "content": base64.b64encode(chunk).decode(),
                    "chunk_index": i,
                    "total_chunks": total,
                }
                msg = Message(message_type=MessageType.COMMAND, payload=payload)
                await ws.send(msg.to_json())
                resp = await self._wait_response(ws, msg.message_id, None, None)
                last = resp
                if not resp.get("success"):
                    return resp
                if resp.get("complete"):
                    break
        if last is None:
            return {"success": False, "error": "no upload response"}
        return last

    async def pull(self, notebook_id: str, remote_path: str,
                   local_path: Optional[str] = None) -> Dict[str, Any]:
        local = local_path or os.path.basename(remote_path)
        payload = {
            "command": "file_download",
            "notebook_id": notebook_id,
            "remote_path": remote_path,
        }
        msg = Message(message_type=MessageType.COMMAND, payload=payload)
        chunks: List[bytes] = []
        meta: Dict[str, Any] = {}

        async with await self._open() as ws:
            await ws.send(msg.to_json())
            while True:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=self.timeout)
                rmsg = Message.from_json(raw)
                if rmsg.correlation_id != msg.message_id:
                    continue
                if rmsg.message_type == MessageType.COMMAND_STREAM:
                    p = rmsg.payload
                    if p.get("kind") == "download_chunk":
                        chunks.append(base64.b64decode(p["content"]))
                        meta.update(p)
                    continue
                if rmsg.message_type == MessageType.COMMAND_RESPONSE:
                    p = rmsg.payload
                    if p.get("success") is False:
                        return p
                    meta.update(p)
                    break

        if not meta.get("complete"):
            return {"success": False,
                    "error": "download did not complete", "meta": meta}
        data = b"".join(chunks)
        expected = meta.get("bytes", len(data))
        digest = hashlib.sha256(data).hexdigest()
        os.makedirs(os.path.dirname(os.path.abspath(local)) or ".", exist_ok=True)
        with open(local, "wb") as fh:
            fh.write(data)
        ok = len(data) == expected
        return {
            "success": ok,
            "local_path": local,
            "remote_path": remote_path,
            "bytes": len(data),
            "sha256": digest,
            "expected_bytes": expected,
            "integrity_ok": ok,
        }

    # ------------------------------------------------------------------ #
    # High-level convenience commands
    # ------------------------------------------------------------------ #
    async def info(self, notebook_id: str) -> Dict[str, Any]:
        cached = await self.request("proxy_info", notebook_id=notebook_id)
        if not cached.get("success"):
            return cached
        summary = cached.get("notebook", {})
        result = {"notebook": summary}
        if summary.get("status") == "connected":
            live = await self.request("system_info", notebook_id=notebook_id)
            if live.get("success"):
                result["info"] = live
        return {"success": True, **result}

    async def execute(self, notebook_id: str, local_path: str,
                      extra_args: Optional[str] = None) -> Dict[str, Any]:
        uploaded = await self.push(notebook_id, local_path)
        if not uploaded.get("success"):
            return uploaded
        filename = os.path.basename(local_path)
        cmd = f"python3 {shlex.quote(filename)}"
        if extra_args:
            cmd += f" {extra_args}"
        return await self.request("shell_exec", notebook_id=notebook_id,
                                  extra={"shell_command": cmd,
                                         "cwd": "."})

    async def logs(self, notebook_id: str,
                   process_id: Optional[str] = None,
                   tail: int = 100) -> Dict[str, Any]:
        if process_id:
            return await self.request("process_logs", notebook_id=notebook_id,
                                      extra={"process_id": process_id,
                                             "tail": tail})
        listing = await self.request("process_list", notebook_id=notebook_id)
        if not listing.get("success"):
            return listing
        processes = listing.get("processes", [])
        out = []
        for proc in processes:
            pl = await self.request("process_logs", notebook_id=notebook_id,
                                    extra={"process_id": proc["process_id"],
                                           "tail": tail})
            out.append({"process": proc, "logs": pl})
        return {"success": True, "processes": out}

    # ------------------------------------------------------------------ #
    # Orchestrated commands (multi-step, agent-friendly)
    # ------------------------------------------------------------------ #
    async def reconnect(self) -> Dict[str, Any]:
        stats = await self.request("proxy_stats")
        listing = await self.request("proxy_list")
        return {
            "success": True,
            "proxy": stats,
            "notebooks": listing.get("notebooks", []),
        }


# ------------------------------------------------------------------------ #
# CLI mapping
# ------------------------------------------------------------------------ #
def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("notebook_id", help="notebook id from `list`")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Control remote Kaggle notebook runners via the testing proxy")
    parser.add_argument(
        "--proxy-url",
        default=os.environ.get("TESTING_PROXY_URL", "ws://localhost:8765/manager"),
        help="manager websocket URL (default: ws://localhost:8765/manager)")
    parser.add_argument("--token", default=os.environ.get("TESTING_PROXY_TOKEN"),
                        help="shared auth token")
    parser.add_argument("--timeout", type=float, default=120.0,
                        help="per-command timeout in seconds")
    parser.add_argument("--pretty", action="store_true",
                        help="indent JSON output for humans")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list connected notebooks")
    sub.add_parser("heartbeat", help="check proxy liveness")
    sub.add_parser("reconnect", help="reconnect to the proxy and report status")

    p = sub.add_parser("info", help="show notebook status + live info")
    _add_common(p)

    p = sub.add_parser("push", help="upload a local file to the notebook")
    p.add_argument("notebook_id")
    p.add_argument("local_path")
    p.add_argument("remote_path", nargs="?", default=None)

    p = sub.add_parser("pull", help="download a remote file")
    p.add_argument("notebook_id")
    p.add_argument("remote_path")
    p.add_argument("local_path", nargs="?", default=None)

    p = sub.add_parser("execute", help="upload and run a python file remotely")
    p.add_argument("notebook_id")
    p.add_argument("local_path")
    p.add_argument("args", nargs="?", default=None,
                   help="extra CLI args passed to the remote script")

    p = sub.add_parser("shell", help="run a shell command")
    _add_common(p)
    p.add_argument("shell_cmd", metavar="command",
                   help="shell command to run")
    p.add_argument("--stream", action="store_true",
                   help="stream output lines as they are produced")
    p.add_argument("--timeout", type=float, default=None,
                   help="command timeout in seconds")

    p = sub.add_parser("subprocess", help="launch a background subprocess")
    _add_common(p)
    p.add_argument("shell_cmd", metavar="command",
                   help="command to launch in the background")

    p = sub.add_parser("terminate", help="terminate a managed process")
    _add_common(p)
    p.add_argument("process_id")

    p = sub.add_parser("ps", help="list managed processes")
    _add_common(p)

    p = sub.add_parser("logs", help="show process logs (all processes or one)")
    _add_common(p)
    p.add_argument("process_id", nargs="?", default=None)
    p.add_argument("--tail", type=int, default=100)

    p = sub.add_parser("pwd", help="print remote working directory")
    _add_common(p)

    p = sub.add_parser("cd", help="change remote working directory")
    _add_common(p)
    p.add_argument("path")

    p = sub.add_parser("mkdir", help="create a remote directory")
    _add_common(p)
    p.add_argument("path")

    p = sub.add_parser("rm", help="remove a remote file or directory")
    _add_common(p)
    p.add_argument("path")

    p = sub.add_parser("ping", help="ping a notebook")
    _add_common(p)

    p = sub.add_parser("system-info", help="fetch live system information")
    _add_common(p)

    env = sub.add_parser("env", help="manage environment variables")
    env_sub = env.add_subparsers(dest="env_command", required=True)

    p = env_sub.add_parser("list", help="list environment variables")
    _add_common(p)

    p = env_sub.add_parser("get", help="get an environment variable")
    _add_common(p)
    p.add_argument("key")

    p = env_sub.add_parser("set", help="set an environment variable")
    _add_common(p)
    p.add_argument("key")
    p.add_argument("value")

    p = env_sub.add_parser("unset", help="unset an environment variable")
    _add_common(p)
    p.add_argument("key")

    return parser


def _command_mapping(args: argparse.Namespace, mgr: InferenceClientManager
                     ) -> Tuple[str, str, Optional[Dict[str, Any]]]:
    """Return (command_name, notebook_id, extra_payload)."""
    nb = getattr(args, "notebook_id", None)
    if args.command == "list":
        return "proxy_list", None, None
    if args.command == "heartbeat":
        return "proxy_heartbeat", None, None
    if args.command == "reconnect":
        return "__reconnect__", None, None
    if args.command == "info":
        return "__info__", nb, None
    if args.command == "push":
        return "__push__", nb, {"local_path": args.local_path,
                                "remote_path": args.remote_path}
    if args.command == "pull":
        return "__pull__", nb, {"remote_path": args.remote_path,
                                "local_path": args.local_path}
    if args.command == "execute":
        return "__execute__", nb, {"local_path": args.local_path,
                                   "args": args.args}
    if args.command == "shell":
        return "shell_exec", nb, {"shell_command": args.shell_cmd,
                                  "stream": args.stream,
                                  "timeout": args.timeout}
    if args.command == "subprocess":
        return "process_start", nb, {"shell_command": args.shell_cmd}
    if args.command == "terminate":
        return "process_terminate", nb, {"process_id": args.process_id}
    if args.command == "ps":
        return "process_list", nb, None
    if args.command == "logs":
        return "logs", nb, {"process_id": args.process_id, "tail": args.tail}
    if args.command == "pwd":
        return "pwd", nb, None
    if args.command == "cd":
        return "cd", nb, {"path": args.path}
    if args.command == "mkdir":
        return "file_mkdir", nb, {"path": args.path}
    if args.command == "rm":
        return "file_delete", nb, {"remote_path": args.path, "recursive": True}
    if args.command == "ping":
        return "notebook_ping", nb, None
    if args.command == "system-info":
        return "system_info", nb, None
    if args.command == "env":
        nb = args.notebook_id
        if args.env_command == "list":
            return "env_list", nb, None
        if args.env_command == "get":
            return "env_get", nb, {"key": args.key}
        if args.env_command == "set":
            return "env_set", nb, {"key": args.key, "value": args.value}
        if args.env_command == "unset":
            return "env_unset", nb, {"key": args.key}
    return "list", None, None


async def _run_command(args: argparse.Namespace,
                       mgr: InferenceClientManager) -> Dict[str, Any]:
    command, nb, extra = _command_mapping(args, mgr)
    if command == "__reconnect__":
        return await mgr.reconnect()
    if command == "__info__":
        return await mgr.info(nb)
    if command == "__push__":
        return await mgr.push(nb, extra["local_path"], extra["remote_path"])
    if command == "__pull__":
        return await mgr.pull(nb, extra["remote_path"], extra["local_path"])
    if command == "__execute__":
        return await mgr.execute(nb, extra["local_path"], extra["args"])
    if command == "logs":
        return await mgr.logs(nb, extra["process_id"], extra["tail"])

    stream_cb = None
    if command == "shell_exec" and args.stream:
        def stream_cb(p: Dict[str, Any]) -> None:
            line = p.get("line", "")
            print(f"[{p.get('stream', 'out')}] {line}", file=sys.stderr)

    return await mgr.request(command, notebook_id=nb, extra=extra,
                             timeout=extra.get("timeout") if extra else None,
                             stream_cb=stream_cb)


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    mgr_timeout = args.timeout if args.timeout is not None else 120.0
    mgr = InferenceClientManager(proxy_url=args.proxy_url, token=args.token,
                                 timeout=mgr_timeout)
    try:
        result = asyncio.run(_run_command(args, mgr))
    except TimeoutError as exc:
        print(json.dumps({"ok": False, "command": args.command,
                          "error": str(exc)}, indent=2 if args.pretty else None))
        return 1
    except (RemoteError, FileNotFoundError, OSError) as exc:
        print(json.dumps({"ok": False, "command": args.command,
                          "error": f"{type(exc).__name__}: {exc}"},
                         indent=2 if args.pretty else None))
        return 1
    except Exception as exc:  # pragma: no cover
        print(json.dumps({"ok": False, "command": args.command,
                          "error": f"{type(exc).__name__}: {exc}"},
                         indent=2 if args.pretty else None))
        return 1

    ok = bool(result.get("success", True))
    output = {"ok": ok, "command": args.command, "result": result}
    print(json.dumps(output, indent=2 if args.pretty else None,
                     default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
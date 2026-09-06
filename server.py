#!/usr/bin/env python3
"""
testing_proxy.py
================
Central WebSocket intermediary for the remote testing framework.

The proxy coordinates two kinds of client:

  * ``manager`` clients (inference_client_manager.py) connect to the
    ``/manager`` path, issue commands, and receive responses/streams.
  * ``runner`` clients (kaggle_notebook_testing_runner.py) connect to the
    ``/notebook`` path, register a stable notebook id, and receive commands.

The proxy is intentionally stateless with respect to notebook *functionality*
(it does not track processes, files or environment). Its only job is reliable
coordination:

  * accept runner connections and assign stable notebook ids
  * maintain a registry of connected notebooks with health state
  * route commands from a manager to the correct notebook
  * route responses and streamed output back to the originating manager
  * preserve request/response correlation ids
  * detect disconnects, queue requests while a notebook is away, and flush
    them when the notebook reconnects
  * track heartbeats and support multiple concurrent managers + notebooks

Usage
-----
    python testing_proxy.py --host 0.0.0.0 --port 8765 \\
        --token optional-shared-secret [--log-level DEBUG]
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import Message, MessageType, CommandType  # noqa: E402

try:
    from websockets.asyncio.server import serve as _serve
    _WS_NEW_API = True
except ImportError:  # pragma: no cover - old websockets (<12)
    try:
        from websockets import serve as _serve  # type: ignore
        _WS_NEW_API = False
    except ImportError:  # pragma: no cover
        _serve = None

if _serve is None:  # pragma: no cover
    raise SystemExit(
        "This program requires the 'websockets' package.\n"
        "Install it with: pip install websockets"
    )

PROXY_COMMANDS = {
    CommandType.PROXY_LIST.value,
    CommandType.PROXY_INFO.value,
    CommandType.PROXY_HEARTBEAT.value,
    CommandType.PROXY_STATS.value,
}

log = logging.getLogger("testing_proxy")


class NotebookConnection:
    """State for a single (possibly reconnecting) notebook runner."""

    def __init__(self, notebook_id: str, secret: str) -> None:
        self.notebook_id = notebook_id
        self.secret = secret
        self.ws: Optional[Any] = None
        self.connected = False
        self.info: Optional[Dict[str, Any]] = None
        self.last_heartbeat: float = time.time()
        self.connected_at: Optional[float] = None
        self.status = "disconnected"
        self.workload: Dict[str, Any] = {}
        self.pending_queue: List[Message] = []

    def summary(self) -> Dict[str, Any]:
        age = None
        if self.connected_at is not None:
            age = round(time.time() - self.connected_at, 2)
        hb_age = round(time.time() - self.last_heartbeat, 2)
        hostname = "-"
        if isinstance(self.info, dict):
            hostname = self.info.get("hostname", "-")
        return {
            "notebook_id": self.notebook_id,
            "status": self.status,
            "hostname": hostname,
            "connected_at": self.connected_at,
            "age_seconds": age,
            "last_heartbeat_age_seconds": hb_age,
            "queued_commands": len(self.pending_queue),
            "workload": self.workload,
        }


class TestingProxy:
    """The persistent coordinator between managers and notebook runners."""

    def __init__(self, host: str, port: int, token: Optional[str] = None,
                 heartbeat_timeout: float = 60.0, queue_max: int = 100,
                 monitor_interval: float = 5.0) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.heartbeat_timeout = heartbeat_timeout
        self.queue_max = queue_max
        self.monitor_interval = monitor_interval

        self.notebooks: Dict[str, NotebookConnection] = {}
        self.managers: Dict[str, Any] = {}
        self.pending: Dict[str, str] = {}

        self.started_at = time.time()
        self.total_commands = 0
        self.total_notebook_connects = 0
        self._lock = asyncio.Lock()
        self._shutting_down = False
        self._server = None

    # ------------------------------------------------------------------ #
    # Server lifecycle
    # ------------------------------------------------------------------ #
    async def serve_forever(self) -> None:
        kwargs = dict(
            handler=self._connection_handler,
            host=self.host,
            port=self.port,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        )
        log.info("testing_proxy listening on ws://%s:%s", self.host, self.port)

        if _WS_NEW_API:
            async with _serve(**kwargs) as _server:
                self._server = _server
                monitor = asyncio.create_task(self._monitor_loop())
                await _server.serve_forever()
                monitor.cancel()
        else:  # pragma: no cover - legacy websockets
            _server = await _serve(**kwargs)
            self._server = _server
            async with _server:
                monitor = asyncio.create_task(self._monitor_loop())
                await _server.serve_forever()
                monitor.cancel()

    def _request_shutdown(self, sig=None) -> None:
        try:
            log.info("received signal %s; shutting down", sig)
        except Exception:
            pass
        self._shutting_down = True
        if self._server is not None:
            try:
                self._server.close()
            except Exception:
                pass

    async def _monitor_loop(self) -> None:
        while not self._shutting_down:
            await asyncio.sleep(self.monitor_interval)
            now = time.time()
            for nb in self.notebooks.values():
                if nb.connected and (now - nb.last_heartbeat) > self.heartbeat_timeout:
                    nb.status = "disconnected"
                    nb.connected = False
                    if nb.ws is not None:
                        old_ws = nb.ws
                        nb.ws = None
                        asyncio.create_task(self._close(old_ws, 1001, "heartbeat timeout"))
                    log.warning(
                        "notebook %s heartbeat stale (%.1fs) -> disconnected",
                        nb.notebook_id, now - nb.last_heartbeat,
                    )
            await self._gc_pending()

    async def _gc_pending(self) -> None:
        async with self._lock:
            dead = [c for c, m in self.pending.items() if m not in self.managers]
            for c in dead:
                del self.pending[c]

    # ------------------------------------------------------------------ #
    # Connection dispatch by URL path
    # ------------------------------------------------------------------ #
    async def _connection_handler(self, ws, path: Optional[str] = None) -> None:
        if path is None:
            try:
                path = getattr(ws.request, "path", "/")
            except Exception:
                path = "/"

        if self.token is not None:
            if self._get_token(ws, path) != self.token:
                log.warning("rejected connection: bad/missing token")
                await self._close(ws, 4001, "unauthorized: bad or missing token")
                return

        if path.startswith("/notebook"):
            await self._handle_notebook(ws)
        elif path.startswith("/manager"):
            await self._handle_manager(ws)
        else:
            await self._close(ws, 1008, "invalid path")
            log.warning("rejected connection at unknown path %s", path)

    def _get_token(self, ws, path: str) -> Optional[str]:
        try:
            qs = getattr(ws.request, "query_string", "")
            qs = qs.decode() if isinstance(qs, (bytes, bytearray)) else qs
            parsed = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
        except Exception:
            parsed = {}
        if "token" not in parsed and "?" in path:
            try:
                query = path.split("?", 1)[1]
                parsed.update(
                    dict(p.split("=", 1) for p in query.split("&") if "=" in p)
                )
            except Exception:
                pass
        return parsed.get("token")

    async def _close(self, ws, code: int, reason: str) -> None:
        try:
            await ws.close(code=code, reason=reason)
        except Exception:
            pass

    async def _send(self, ws, message: Message) -> None:
        if ws is None:
            return
        try:
            await ws.send(message.to_json())
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Notebook (runner) connections
    # ------------------------------------------------------------------ #
    async def _handle_notebook(self, ws) -> None:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
        except Exception as exc:
            log.warning("notebook register timeout/error: %s", exc)
            await self._close(ws, 1008, "register timeout")
            return

        try:
            reg = Message.from_json(raw)
        except json.JSONDecodeError:
            log.warning("unparseable register message from notebook")
            await self._close(ws, 1008, "unparseable register message")
            return

        if reg.message_type != MessageType.REGISTER:
            log.warning("first message from notebook was %s, expected REGISTER", reg.message_type)
            await self._close(ws, 1008, "first message must be register")
            return

        payload = reg.payload or {}
        given_id = payload.get("notebook_id") or "auto"
        secret = payload.get("secret") or ""
        info = payload.get("info")

        log.info("notebook registering given_id=%s secret_provided=%s info=%s",
                 given_id, bool(secret), bool(info))

        async with self._lock:
            existing = self.notebooks.get(given_id)
            if existing is not None:
                if existing.secret and secret != existing.secret:
                    log.warning("rejected notebook %s: bad secret", given_id)
                    await self._close(ws, 4001, "unauthorized: bad secret")
                    return
                nb = existing
                resumed = True
                if nb.connected and nb.ws is not None and nb.ws is not ws:
                    log.info("superseding old WebSocket for notebook %s", nb.notebook_id)
                    old_ws = nb.ws
                    asyncio.create_task(self._close(old_ws, 4001, "superseded by new connection"))
                nb.ws = ws
                nb.connected = True
                nb.status = "connected"
                nb.last_heartbeat = time.time()
                nb.connected_at = time.time()
                if isinstance(info, dict):
                    nb.info = info
                queued = list(nb.pending_queue)
                nb.pending_queue.clear()
                self.total_notebook_connects += 1
                log.info("resumed registered notebook: %s (flushing %d queued commands)",
                         nb.notebook_id, len(queued))
            else:
                nb_id = given_id if given_id != "auto" else self._new_id()
                nb = NotebookConnection(nb_id, secret or self._new_secret())
                self.notebooks[nb_id] = nb
                nb.connected = True
                nb.status = "connected"
                nb.last_heartbeat = time.time()
                nb.connected_at = time.time()
                nb.info = info if isinstance(info, dict) else None
                resumed = False
                queued = []
                self.total_notebook_connects += 1
                log.info("registered new notebook: %s (secret=%s)", nb.notebook_id, nb.secret[:6] + "...")

            ack = Message(
                message_type=MessageType.REGISTER_ACK,
                payload={
                    "notebook_id": nb.notebook_id,
                    "secret": nb.secret,
                    "resumed": resumed,
                    "status": "connected",
                    "queued_flushed": len(queued),
                    "server_time": time.time(),
                },
                correlation_id=reg.message_id,
            )
            await self._send(ws, ack)

        for qmsg in queued:
            log.info("flushing queued command %s to notebook %s", qmsg.message_id[:8], nb.notebook_id)
            await self._send(ws, qmsg)
            self.total_commands += 1

        try:
            async for raw in ws:
                try:
                    msg = Message.from_json(raw)
                except json.JSONDecodeError:
                    log.warning("unparseable message from notebook %s", nb.notebook_id)
                    continue
                log.info("[RECV notebook %s] msg_type=%s id=%s corr=%s",
                         nb.notebook_id, msg.message_type.value, msg.message_id[:8], (msg.correlation_id or "-")[:8])
                await self._handle_notebook_message(nb, msg)
        except Exception as exc:
            log.warning("notebook %s connection loop error: %s", nb.notebook_id, exc)
        finally:
            nb.status = "disconnected"
            if nb.ws is ws:
                nb.connected = False
                nb.ws = None
            log.info("notebook connection closed: %s (connected=%s)", nb.notebook_id, nb.connected)

    async def _handle_notebook_message(self, nb: NotebookConnection, msg: Message) -> None:
        if msg.message_type == MessageType.HEARTBEAT:
            now = time.time()
            dt = now - nb.last_heartbeat
            nb.last_heartbeat = now
            p = msg.payload or {}
            if isinstance(p.get("info"), dict):
                nb.info = p["info"]
            if isinstance(p.get("workload"), dict):
                nb.workload = p["workload"]
            log.info("[HEARTBEAT notebook %s] updated heartbeat (dt=%.2fs, workload=%s)",
                     nb.notebook_id, dt, nb.workload)
            await self._send(
                nb.ws,
                Message(
                    message_type=MessageType.HEARTBEAT_ACK,
                    correlation_id=msg.message_id,
                    payload={"server_time": time.time()},
                ),
            )
        elif msg.message_type in (MessageType.COMMAND_RESPONSE,
                                  MessageType.COMMAND_STREAM):
            await self._route_notebook_response(nb, msg)

    async def _route_notebook_response(self, nb: NotebookConnection, msg: Message) -> None:
        corr = msg.correlation_id
        async with self._lock:
            mgr_id = self.pending.get(corr)
        if mgr_id is None:
            return
        mgr_ws = self.managers.get(mgr_id)
        if mgr_ws is None:
            async with self._lock:
                self.pending.pop(corr, None)
            return
        await self._send(mgr_ws, msg)
        if msg.message_type == MessageType.COMMAND_RESPONSE:
            async with self._lock:
                self.pending.pop(corr, None)

    # ------------------------------------------------------------------ #
    # Manager connections
    # ------------------------------------------------------------------ #
    async def _handle_manager(self, ws) -> None:
        mgr_id = str(uuid.uuid4())[:8]
        self.managers[mgr_id] = ws
        log.info("manager %s connected (%d active)", mgr_id, len(self.managers))
        try:
            async for raw in ws:
                try:
                    msg = Message.from_json(raw)
                except json.JSONDecodeError:
                    continue
                await self._handle_manager_message(mgr_id, ws, msg)
        except Exception:
            pass
        finally:
            self.managers.pop(mgr_id, None)
            async with self._lock:
                dead = [c for c, m in self.pending.items() if m == mgr_id]
                for c in dead:
                    del self.pending[c]
            log.info("manager %s disconnected (%d active)", mgr_id, len(self.managers))

    async def _handle_manager_message(self, mgr_id: str, ws, msg: Message) -> None:
        mtype = msg.message_type
        if mtype == MessageType.HEARTBEAT:
            await self._send(
                ws,
                Message(
                    message_type=MessageType.HEARTBEAT_ACK,
                    correlation_id=msg.message_id,
                    payload={"server_time": time.time()},
                ),
            )
            return
        if mtype == MessageType.MANAGER_HELLO:
            await self._send(
                ws,
                Message(
                    message_type=MessageType.REGISTER_ACK,
                    correlation_id=msg.message_id,
                    payload={"manager_id": mgr_id, "status": "connected"},
                ),
            )
            return
        if mtype != MessageType.COMMAND:
            return

        payload = msg.payload or {}
        command = payload.get("command")
        notebook_id = payload.get("notebook_id")

        if command in PROXY_COMMANDS or not notebook_id:
            await self._handle_proxy_command(mgr_id, ws, msg, command, payload)
        else:
            await self._route_command_to_notebook(mgr_id, ws, msg, notebook_id)

    async def _handle_proxy_command(self, mgr_id: str, ws, msg: Message,
                                    command: str, payload: Dict[str, Any]) -> None:
        crafted = None
        if command == CommandType.PROXY_LIST.value:
            crafted = Message.create_response(msg, {
                "notebooks": [nb.summary() for nb in self.notebooks.values()],
            })
        elif command == CommandType.PROXY_INFO.value:
            nb = self.notebooks.get(payload.get("notebook_id"))
            if nb is None:
                crafted = Message.create_response(msg, {
                    "success": False, "error": "unknown notebook_id",
                })
            else:
                crafted = Message.create_response(msg, {
                    "success": True, "notebook": nb.summary(),
                })
        elif command == CommandType.PROXY_HEARTBEAT.value:
            connected = sum(1 for nb in self.notebooks.values() if nb.connected)
            crafted = Message.create_response(msg, {
                "status": "ok", "server_time": time.time(),
                "uptime_seconds": round(time.time() - self.started_at, 2),
                "notebooks_total": len(self.notebooks),
                "notebooks_connected": connected,
            })
        elif command == CommandType.PROXY_STATS.value:
            connected = sum(1 for nb in self.notebooks.values() if nb.connected)
            crafted = Message.create_response(msg, {
                "uptime_seconds": round(time.time() - self.started_at, 2),
                "notebooks_total": len(self.notebooks),
                "notebooks_connected": connected,
                "managers_active": len(self.managers),
                "pending_routes": len(self.pending),
                "total_commands": self.total_commands,
                "total_notebook_connects": self.total_notebook_connects,
            })
        else:
            crafted = Message.create_response(msg, {
                "success": False, "error": f"unsupported proxy command {command!r}",
            })

        if crafted is not None:
            await self._send(ws, crafted)

    async def _route_command_to_notebook(self, mgr_id: str, ws, msg: Message,
                                         notebook_id: str) -> None:
        nb = self.notebooks.get(notebook_id)
        corr = msg.message_id

        if nb is None:
            log.warning("cannot route command %s: unknown notebook_id %s", corr[:8], notebook_id)
            await self._send(ws, Message.create_response(msg, {
                "success": False,
                "error": f"unknown notebook_id {notebook_id!r}",
                "notebook_id": notebook_id,
            }))
            return

        hb_age = round(time.time() - nb.last_heartbeat, 2)
        log.info("[ROUTE command %s -> %s] connected=%s ws_present=%s hb_age=%.2fs pending_queue=%d",
                 corr[:8], notebook_id, nb.connected, nb.ws is not None, hb_age, len(nb.pending_queue))

        async with self._lock:
            self.pending[corr] = mgr_id
            self.total_commands += 1
            if nb.connected and nb.ws is not None:
                target_ws = nb.ws
                should_queue = None
            else:
                target_ws = None
                should_queue = msg

        if target_ws is not None:
            try:
                log.info("sending command %s directly to notebook %s WebSocket", corr[:8], notebook_id)
                await target_ws.send(msg.to_json())
                return
            except Exception as exc:
                log.warning(
                    "failed to send command %s to notebook %s (%s); marking offline and queuing",
                    corr[:8], notebook_id, exc,
                )
                async with self._lock:
                    nb.connected = False
                    nb.status = "disconnected"
                    nb.ws = None
                    should_queue = msg

        if should_queue is not None:
            if len(nb.pending_queue) >= self.queue_max:
                async with self._lock:
                    self.pending.pop(corr, None)
                await self._send(ws, Message.create_response(msg, {
                    "success": False, "error": "queue full",
                    "notebook_id": notebook_id,
                }))
                return
            nb.pending_queue.append(should_queue)
            log.info(
                "notebook %s offline; queued command %s (depth %d)",
                notebook_id, corr, len(nb.pending_queue),
            )
            await self._send(ws, Message(
                message_type=MessageType.COMMAND_QUEUED,
                correlation_id=corr,
                payload={
                    "status": "queued",
                    "notebook_id": notebook_id,
                    "queued_depth": len(nb.pending_queue),
                },
            ))

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _new_id() -> str:
        return "nb-" + uuid.uuid4().hex[:10]

    @staticmethod
    def _new_secret() -> str:
        return uuid.uuid4().hex


async def _run(proxy: TestingProxy) -> None:
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, proxy._request_shutdown, sig)
        except NotImplementedError:
            pass
    await proxy.serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser(description="Remote testing WebSocket proxy")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--token", default=None, help="shared auth token (optional)")
    parser.add_argument("--heartbeat-timeout", type=float, default=60.0)
    parser.add_argument("--queue-max", type=int, default=100)
    parser.add_argument("--monitor-interval", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    proxy = TestingProxy(
        host=args.host, port=args.port, token=args.token,
        heartbeat_timeout=args.heartbeat_timeout, queue_max=args.queue_max,
        monitor_interval=args.monitor_interval,
    )

    async def _restartable() -> None:
        while True:
            try:
                await _run(proxy)
                return
            except OSError as exc:
                if proxy._shutting_down:
                    return
                log.error("server error: %s; restarting in 1s", exc)
                await asyncio.sleep(1)

    try:
        asyncio.run(_restartable())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import mimetypes
import ssl
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

import aiohttp

try:
    from astrbot.api.star import StarTools
except ImportError:  # pragma: no cover - fallback for non-AstrBot runtime
    StarTools = None


DEFAULT_WS_URL = "wss://hag.cloud.huawei.com/openclaw/v1/ws/link"
DEFAULT_WS_URL_2 = "wss://116.63.174.231/openclaw/v1/ws/link"
DEFAULT_PUSH_URL = "https://hag.cloud.huawei.com/open-ability-agent/v1/agent-webhook"
PLUGIN_NAME = "astrbot_plugin_xiaoyi_adapter"
LEGACY_PERSISTED_PUSH_ID_FILE = Path(__file__).resolve().parent / ".data" / "session_push_ids.json"


def resolve_persisted_push_id_file() -> Path:
    if StarTools is not None:
        try:
            return StarTools.get_data_dir(PLUGIN_NAME) / "session_push_ids.json"
        except Exception:
            pass
    return LEGACY_PERSISTED_PUSH_ID_FILE


class XiaoYiClient:
    def __init__(
        self,
        ws_url: str,
        ak: str,
        sk: str,
        agent_id: str,
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        logger: Any,
        ws_url2: str | None = None,
        api_id: str | None = None,
        push_id: str | None = None,
        push_url: str | None = None,
        session_cleanup_delay_ms: int = 300000,
        session_state_ttl_ms: int = 3600000,
        reconnect_base_delay: float = 2.0,
        reconnect_max_delay: float = 60.0,
        heartbeat_interval: float = 30.0,
        app_heartbeat_interval: float = 20.0,
    ) -> None:
        self.ws_url = ws_url or DEFAULT_WS_URL
        self.ws_url2 = ws_url2 or DEFAULT_WS_URL_2
        self.ak = ak
        self.sk = sk
        self.agent_id = agent_id
        self.on_message = on_message
        self.logger = logger
        self.api_id = api_id or ""
        self.default_push_id = push_id or ""
        self.push_url = push_url or DEFAULT_PUSH_URL
        self.session_cleanup_delay_ms = max(0, int(session_cleanup_delay_ms))
        self.session_state_ttl_ms = max(0, int(session_state_ttl_ms))
        self.reconnect_base_delay = reconnect_base_delay
        self.reconnect_max_delay = reconnect_max_delay
        self.heartbeat_interval = heartbeat_interval
        self.app_heartbeat_interval = app_heartbeat_interval

        self._stopped = False
        self._server_defs: dict[str, str] = {"server1": self.ws_url}
        if self.ws_url2:
            self._server_defs["server2"] = self.ws_url2

        self._connections: dict[str, aiohttp.ClientWebSocketResponse | None] = {
            server_id: None for server_id in self._server_defs
        }
        self._send_locks: dict[str, asyncio.Lock] = {
            server_id: asyncio.Lock() for server_id in self._server_defs
        }
        self._runner_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._app_heartbeat_task: asyncio.Task | None = None

        self._session_server_map: dict[str, str] = {}
        self._session_push_id_map: dict[str, str] = {}
        self._session_last_seen_at: dict[str, float] = {}
        self._session_cleanup_tasks: dict[str, asyncio.Task] = {}
        self._persisted_push_id_file = resolve_persisted_push_id_file()
        self._load_persisted_push_ids()

    def _load_persisted_push_ids(self) -> None:
        try:
            if not self._persisted_push_id_file.exists():
                return
            raw = json.loads(self._persisted_push_id_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.logger.warning(
                    "XiaoYi push id cache is invalid, expected object: %s",
                    self._persisted_push_id_file,
                )
                return
            loaded = 0
            for session_id, push_id in raw.items():
                if isinstance(session_id, str) and isinstance(push_id, str) and session_id and push_id.strip():
                    self._session_push_id_map[session_id] = push_id.strip()
                    loaded += 1
            if loaded:
                self.logger.info("XiaoYi restored %s persisted session push id(s)", loaded)
        except Exception as exc:
            self.logger.warning("XiaoYi failed to load persisted push ids: %s", exc)

    def _save_persisted_push_ids(self) -> None:
        try:
            self._persisted_push_id_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                session_id: push_id
                for session_id, push_id in self._session_push_id_map.items()
                if session_id and push_id
            }
            self._persisted_push_id_file.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            self.logger.warning("XiaoYi failed to persist push ids: %s", exc)

    def _update_session_push_id(self, session_id: str, push_id: str) -> None:
        normalized_push_id = push_id.strip()
        if not session_id or not normalized_push_id:
            return
        previous = self._session_push_id_map.get(session_id)
        self._session_push_id_map[session_id] = normalized_push_id
        if previous != normalized_push_id:
            self.logger.info("XiaoYi cached push_id for session %s", session_id)
            self._save_persisted_push_ids()

    def _build_headers(self) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        digest = hmac.new(self.sk.encode("utf-8"), ts.encode("utf-8"), hashlib.sha256).digest()
        sign = base64.b64encode(digest).decode("ascii")
        return {
            "x-access-key": self.ak,
            "x-sign": sign,
            "x-ts": ts,
            "x-agent-id": self.agent_id,
        }

    def _generate_signature(self, timestamp: str) -> str:
        digest = hmac.new(self.sk.encode("utf-8"), timestamp.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("ascii")

    def _build_ws_ssl_context(self, url: str) -> ssl.SSLContext | bool | None:
        parsed = urlparse(url)
        if parsed.scheme != "wss":
            return None
        hostname = parsed.hostname or ""
        try:
            ipaddress.ip_address(hostname)
        except ValueError:
            return None

        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.logger.info("XiaoYi websocket %s uses WSS + IP, certificate verification disabled", url)
        return context

    def _pick_send_server(self, session_id: str | None = None, preferred_server: str | None = None) -> str:
        if preferred_server and self._connections.get(preferred_server) and not self._connections[preferred_server].closed:
            return preferred_server
        if preferred_server:
            raise RuntimeError(f"Preferred XiaoYi websocket {preferred_server} is not connected")

        if session_id:
            bound = self._session_server_map.get(session_id)
            if bound and self._connections.get(bound) and not self._connections[bound].closed:
                return bound
            if bound:
                raise RuntimeError(
                    f"XiaoYi session {session_id} is bound to unavailable websocket {bound}; refusing to reroute"
                )

        for server_id, ws in self._connections.items():
            if ws and not ws.closed:
                return server_id

        raise RuntimeError("No XiaoYi websocket connection is available")

    def _touch_session(self, session_id: str) -> None:
        self._session_last_seen_at[session_id] = time.time()
        cleanup_task = self._session_cleanup_tasks.pop(session_id, None)
        if cleanup_task:
            cleanup_task.cancel()

    def _clear_session_state(self, session_id: str) -> None:
        self._session_server_map.pop(session_id, None)
        self._session_last_seen_at.pop(session_id, None)
        cleanup_task = self._session_cleanup_tasks.pop(session_id, None)
        if cleanup_task:
            cleanup_task.cancel()

    async def _cleanup_session_after_delay(self, session_id: str, delay_ms: int) -> None:
        try:
            await asyncio.sleep(max(0, delay_ms) / 1000)
            self._clear_session_state(session_id)
        except asyncio.CancelledError:
            raise

    def mark_session_for_cleanup(self, session_id: str, delay_ms: int | None = None) -> None:
        existing = self._session_cleanup_tasks.pop(session_id, None)
        if existing:
            existing.cancel()
        delay = self.session_cleanup_delay_ms if delay_ms is None else int(delay_ms)
        self._session_cleanup_tasks[session_id] = asyncio.create_task(
            self._cleanup_session_after_delay(session_id, delay)
        )

    def prune_stale_sessions(self) -> None:
        if self.session_state_ttl_ms <= 0:
            return
        now = time.time()
        ttl_seconds = self.session_state_ttl_ms / 1000
        for session_id, last_seen in list(self._session_last_seen_at.items()):
            if now - last_seen > ttl_seconds:
                self._clear_session_state(session_id)

    async def _send_json(
        self,
        payload: dict[str, Any],
        session_id: str | None = None,
        preferred_server: str | None = None,
    ) -> None:
        server_id = self._pick_send_server(session_id=session_id, preferred_server=preferred_server)
        async with self._send_locks[server_id]:
            ws = self._connections.get(server_id)
            if not ws or ws.closed:
                raise RuntimeError(f"XiaoYi websocket {server_id} is not connected")
            await ws.send_str(json.dumps(payload, ensure_ascii=False))

    async def _send_init(self, server_id: str) -> None:
        await self._send_json(
            {
                "msgType": "clawd_bot_init",
                "agentId": self.agent_id,
            },
            preferred_server=server_id,
        )

    async def _heartbeat_loop(self, server_id: str) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(self.heartbeat_interval)
                await self._send_json(
                    {
                        "msgType": "heartbeat",
                        "agentId": self.agent_id,
                        "timestamp": int(time.time() * 1000),
                    },
                    preferred_server=server_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.warning("XiaoYi heartbeat stopped on %s: %s", server_id, exc)

    async def _app_heartbeat_loop(self) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(self.app_heartbeat_interval)
                for server_id, ws in list(self._connections.items()):
                    if not ws or ws.closed:
                        continue
                    try:
                        await self._send_json(
                            {
                                "msgType": "heartbeat",
                                "agentId": self.agent_id,
                            },
                            preferred_server=server_id,
                        )
                    except Exception as exc:
                        self.logger.warning("XiaoYi app heartbeat failed on %s: %s", server_id, exc)
        except asyncio.CancelledError:
            raise

    def _extract_session_id(self, payload: dict[str, Any]) -> str | None:
        params = payload.get("params", {})
        return params.get("sessionId") or payload.get("sessionId")

    def _extract_push_id(self, payload: dict[str, Any]) -> str | None:
        parts = ((payload.get("params") or {}).get("message") or {}).get("parts") or []
        for part in parts:
            if part.get("kind") != "data":
                continue
            data = part.get("data") or {}
            push_id = (((data.get("variables") or {}).get("systemVariables") or {}).get("push_id"))
            if isinstance(push_id, str) and push_id.strip():
                return push_id.strip()
        return None

    async def _run_server(self, server_id: str, url: str) -> None:
        retry = 0
        while not self._stopped:
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
                ssl_context = self._build_ws_ssl_context(url)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    self.logger.info("Connecting XiaoYi websocket %s: %s", server_id, url)
                    async with session.ws_connect(
                        url,
                        headers=self._build_headers(),
                        heartbeat=None,
                        autoping=True,
                        ssl=ssl_context,
                    ) as ws:
                        self._connections[server_id] = ws
                        retry = 0
                        await self._send_init(server_id)
                        heartbeat_task = asyncio.create_task(self._heartbeat_loop(server_id))
                        self._heartbeat_tasks[server_id] = heartbeat_task
                        self.logger.info("XiaoYi websocket %s connected", server_id)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    self.logger.warning("Ignore non-JSON XiaoYi payload on %s: %s", server_id, msg.data)
                                    continue

                                session_id = self._extract_session_id(payload)
                                if session_id:
                                    self.prune_stale_sessions()
                                    self._touch_session(session_id)
                                    self._session_server_map[session_id] = server_id
                                    push_id = self._extract_push_id(payload)
                                    if push_id:
                                        self._update_session_push_id(session_id, push_id)

                                await self.on_message(payload)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise ws.exception() or RuntimeError(f"XiaoYi websocket {server_id} error")
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry += 1
                delay = min(self.reconnect_base_delay * (2 ** (retry - 1)), self.reconnect_max_delay)
                self.logger.warning(
                    "XiaoYi websocket %s disconnected: %s; retry in %.1fs",
                    server_id,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
            finally:
                task = self._heartbeat_tasks.pop(server_id, None)
                if task:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                self._connections[server_id] = None

    async def run_forever(self) -> None:
        self._stopped = False
        self._runner_tasks = {
            server_id: asyncio.create_task(self._run_server(server_id, url))
            for server_id, url in self._server_defs.items()
        }
        self._app_heartbeat_task = asyncio.create_task(self._app_heartbeat_loop())
        await asyncio.gather(*self._runner_tasks.values())

    async def stop(self) -> None:
        self._stopped = True
        for task in self._heartbeat_tasks.values():
            task.cancel()
        if self._app_heartbeat_task:
            self._app_heartbeat_task.cancel()
        for task in self._runner_tasks.values():
            task.cancel()
        for task in self._session_cleanup_tasks.values():
            task.cancel()
        for task in list(self._heartbeat_tasks.values()) + list(self._runner_tasks.values()):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._app_heartbeat_task:
            try:
                await self._app_heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._app_heartbeat_task = None
        for task in list(self._session_cleanup_tasks.values()):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        for server_id, ws in self._connections.items():
            if ws and not ws.closed:
                try:
                    await ws.close()
                except Exception:
                    pass
            self._connections[server_id] = None
        self._session_server_map.clear()
        self._session_push_id_map.clear()
        self._session_last_seen_at.clear()
        self._session_cleanup_tasks.clear()

    def is_push_configured(self, session_id: str | None = None) -> bool:
        return bool(
            self.api_id
            and self.ak
            and self.sk
            and self.get_push_id(session_id).strip()
        )

    def get_push_id(self, session_id: str | None = None) -> str:
        if session_id:
            return self._session_push_id_map.get(session_id, "") or self.default_push_id
        return self.default_push_id

    def get_push_config_diagnostics(self, session_id: str | None = None) -> dict[str, Any]:
        session_push_id = self._session_push_id_map.get(session_id or "", "") if session_id else ""
        effective_push_id = self.get_push_id(session_id).strip()
        return {
            "has_api_id": bool(self.api_id),
            "has_ak": bool(self.ak),
            "has_sk": bool(self.sk),
            "has_default_push_id": bool(self.default_push_id.strip()),
            "has_session_push_id": bool(session_push_id.strip()),
            "effective_push_id_source": (
                "session"
                if session_push_id.strip()
                else "default"
                if self.default_push_id.strip()
                else "none"
            ),
            "is_push_configured": bool(
                self.api_id and self.ak and self.sk and effective_push_id
            ),
        }

    async def send_push_notification(
        self,
        session_id: str | None,
        text: str,
        title: str | None = None,
    ) -> bool:
        push_id = self.get_push_id(session_id)
        if not (self.api_id and push_id and self.ak and self.sk):
            return False

        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp)
        trace_id = f"push_{int(time.time() * 1000)}"
        summary = (text or "").strip()[:1000]
        payload = {
            "jsonrpc": "2.0",
            "id": trace_id,
            "result": {
                "id": trace_id,
                "apiId": self.api_id,
                "pushId": push_id,
                "pushText": (title or summary or "Task completed")[:57],
                "kind": "task",
                "artifacts": [
                    {
                        "artifactId": f"artifact_{int(time.time() * 1000)}",
                        "parts": [{"kind": "text", "text": summary}],
                    }
                ],
                "status": {"state": "completed"},
            },
        }

        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.push_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "x-hag-trace-id": trace_id,
                        "X-Access-Key": self.ak,
                        "X-Sign": signature,
                        "X-Ts": timestamp,
                    },
                ) as response:
                    if response.status < 300:
                        self.logger.info("XiaoYi push notification sent for session %s", session_id)
                        return True
                    self.logger.warning("XiaoYi push notification failed: HTTP %s", response.status)
                    return False
        except Exception as exc:
            self.logger.warning("XiaoYi push notification error: %s", exc)
            return False

    async def send_text_response(
        self,
        session_id: str,
        task_id: str,
        request_id: str,
        text: str,
    ) -> None:
        await self.send_artifact_update(
            session_id=session_id,
            task_id=task_id,
            request_id=request_id,
            parts=[{"kind": "text", "text": text}],
            append=False,
            final=True,
        )

    async def send_artifact_update(
        self,
        session_id: str,
        task_id: str,
        request_id: str,
        parts: list[dict[str, Any]],
        append: bool,
        final: bool,
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "taskId": task_id,
                "kind": "artifact-update",
                "append": append,
                "lastChunk": True,
                "final": final,
                "artifact": {
                    "artifactId": f"artifact_{int(time.time() * 1000)}",
                    "parts": parts,
                },
            },
        }
        self.logger.info(
            "Sending XiaoYi artifact-update: session=%s task=%s final=%s append=%s parts=%s",
            session_id,
            task_id,
            final,
            append,
            [part.get("kind") for part in parts],
        )
        await self._send_json(
            {
                "msgType": "agent_response",
                "agentId": self.agent_id,
                "sessionId": session_id,
                "taskId": task_id,
                "msgDetail": json.dumps(payload, ensure_ascii=False),
            },
            session_id=session_id,
        )

    async def send_stream_text(
        self,
        session_id: str,
        task_id: str,
        request_id: str,
        text: str,
        append: bool,
    ) -> None:
        await self.send_artifact_update(
            session_id=session_id,
            task_id=task_id,
            request_id=request_id,
            parts=[{"kind": "reasoningText", "reasoningText": text}],
            append=append,
            final=False,
        )

    async def finalize_stream_response(
        self,
        session_id: str,
        task_id: str,
        request_id: str,
    ) -> None:
        await self.send_artifact_update(
            session_id=session_id,
            task_id=task_id,
            request_id=request_id,
            parts=[{"kind": "text", "text": ""}],
            append=True,
            final=True,
        )

    @staticmethod
    def build_file_part_from_url(url: str, mime_type: str | None = None, file_name: str | None = None) -> dict[str, Any]:
        final_mime = mime_type or mimetypes.guess_type(file_name or url)[0] or "application/octet-stream"
        final_name = file_name or Path(url.split("?", 1)[0]).name or "file"
        return {
            "kind": "file",
            "file": {
                "name": final_name,
                "mimeType": final_mime,
                "uri": url,
            },
        }

    @staticmethod
    def build_file_part_from_bytes(data: bytes, file_name: str, mime_type: str | None = None) -> dict[str, Any]:
        final_mime = mime_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        return {
            "kind": "file",
            "file": {
                "name": file_name,
                "mimeType": final_mime,
                "bytes": base64.b64encode(data).decode("ascii"),
            },
        }

    @staticmethod
    def build_data_part(data: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "data", "data": data}

    @staticmethod
    def build_reasoning_text_part(text: str) -> dict[str, Any]:
        return {"kind": "reasoningText", "reasoningText": text}

    @staticmethod
    def build_command_part(command: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "data", "data": {"commands": [command]}}

    async def send_status_update(
        self,
        session_id: str,
        task_id: str,
        request_id: str,
        text: str,
        state: str = "working",
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "taskId": task_id,
                "kind": "status-update",
                "final": False,
                "status": {
                    "message": {
                        "role": "agent",
                        "parts": [{"kind": "text", "text": text}],
                    },
                    "state": state,
                },
            },
        }
        self.logger.info(
            "Sending XiaoYi status-update: session=%s task=%s state=%s text=%s",
            session_id,
            task_id,
            state,
            text[:120],
        )
        await self._send_json(
            {
                "msgType": "agent_response",
                "agentId": self.agent_id,
                "sessionId": session_id,
                "taskId": task_id,
                "msgDetail": json.dumps(payload, ensure_ascii=False),
            },
            session_id=session_id,
        )

    async def send_clear_context_response(self, session_id: str, request_id: str) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "status": {
                    "state": "cleared",
                }
            },
        }
        await self._send_json(
            {
                "msgType": "agent_response",
                "agentId": self.agent_id,
                "sessionId": session_id,
                "taskId": session_id,
                "msgDetail": json.dumps(payload, ensure_ascii=False),
            },
            session_id=session_id,
        )

    async def send_cancel_response(self, session_id: str, task_id: str, request_id: str) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "id": task_id,
                "status": {
                    "state": "canceled",
                },
            },
        }
        await self._send_json(
            {
                "msgType": "agent_response",
                "agentId": self.agent_id,
                "sessionId": session_id,
                "taskId": task_id,
                "msgDetail": json.dumps(payload, ensure_ascii=False),
            },
            session_id=session_id,
        )

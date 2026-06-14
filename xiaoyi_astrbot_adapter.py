import asyncio
from typing import Any, Optional

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Image, Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.platform.astr_message_event import MessageSesion
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.utils.active_event_registry import active_event_registry

from .main import get_active_plugin_context
from .xiaoyi_astrbot_event import UNSUPPORTED_MESSAGE_NOTICE, XiaoYiAstrBotEvent
from .xiaoyi_client import DEFAULT_WS_URL, XiaoYiClient
from .xiaoyi_config import CONFIG_METADATA, DEFAULT_CONFIG_TMPL, I18N_RESOURCES

_ACTIVE_XIAOYI_CLIENT: XiaoYiClient | None = None


def get_active_xiaoyi_client() -> XiaoYiClient | None:
    return _ACTIVE_XIAOYI_CLIENT


async def _reset_astrbot_session(session_id: str, platform_id: str) -> None:
    context = get_active_plugin_context()
    if context is None:
        logger.warning(
            "[XiaoYi Adapter] clearContext received for session %s but plugin context is unavailable",
            session_id,
        )
        return

    umo = str(MessageSession(platform_id, MessageType.FRIEND_MESSAGE, session_id))

    try:
        active_event_registry.stop_all(umo)

        cfg = context.get_config(umo=umo)
        agent_runner_type = cfg.get("provider_settings", {}).get("agent_runner_type", "")

        from astrbot.builtin_stars.builtin_commands.commands.conversation import (
            THIRD_PARTY_AGENT_RUNNER_KEY,
            _clear_third_party_agent_runner_state,
        )

        if agent_runner_type in THIRD_PARTY_AGENT_RUNNER_KEY:
            await _clear_third_party_agent_runner_state(
                context,
                umo,
                agent_runner_type,
            )
        else:
            cid = await context.conversation_manager.get_curr_conversation_id(umo)
            if cid:
                await context.conversation_manager.update_conversation(umo, cid, [])

        logger.info(
            "[XiaoYi Adapter] clearContext synced to AstrBot reset: session=%s umo=%s",
            session_id,
            umo,
        )
    except Exception as exc:
        logger.warning(
            "[XiaoYi Adapter] failed to sync clearContext to AstrBot reset: session=%s error=%s",
            session_id,
            exc,
        )


@register_platform_adapter(
    "xiaoyi",
    "XiaoYi adapter",
    default_config_tmpl=DEFAULT_CONFIG_TMPL,
    config_metadata=CONFIG_METADATA,
    i18n_resources=I18N_RESOURCES,
    support_streaming_message=True,
    adapter_display_name="XiaoYi",
    logo_path="logo.png"
)
class XiaoYiAstrBotAdapter(Platform):
    def __init__(self, platform_config: dict, platform_settings: dict, event_queue: asyncio.Queue):
        super().__init__(platform_config, event_queue)
        self.config = platform_config
        self.settings = platform_settings
        self.client: Optional[XiaoYiClient] = None
        self._running = False
        self._validate_config(platform_config)
        logger.info("[XiaoYi Adapter] 初始化完成")
    def _validate_config(self, config: dict) -> None:
        required = ("ak", "sk", "agentId")
        for key in required:
            if not config.get(key):
                raise ValueError(f"XiaoYi adapter missing required config: {key}")

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="xiaoyi",
            description="XiaoYi Adapter",
            id=self.config.get("id"),
            adapter_display_name="XiaoYi",
            support_proactive_message=True,
        )

    async def send_by_session(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        if not self.client:
            logger.warning("[XiaoYi Adapter] proactive send skipped: client not running")
            return

        if not self.client.is_push_configured():
            logger.warning(
                "[XiaoYi Adapter] proactive send skipped: push not configured for session %s",
                session.session_id,
            )
            return

        chain = message_chain.chain if isinstance(message_chain, MessageChain) else message_chain
        plain_parts: list[str] = []
        ignored_non_text_found = False

        for item in chain:
            if isinstance(item, Plain):
                if item.text:
                    plain_parts.append(item.text)
                continue

            if isinstance(item, Image):
                ignored_non_text_found = True
                plain_parts.append("[Image]")
                continue

            if hasattr(item, "text") and isinstance(getattr(item, "text"), str):
                plain_parts.append(getattr(item, "text"))
                continue

            ignored_non_text_found = True
            plain_parts.append(f"[{item.__class__.__name__}]")

        text = "".join(plain_parts).strip()
        if not text and ignored_non_text_found:
            text = UNSUPPORTED_MESSAGE_NOTICE
        if not text:
            logger.info(
                "[XiaoYi Adapter] proactive send ignored empty message for session %s",
                session.session_id,
            )
            return

        title = text.splitlines()[0][:57]
        sent = await self.client.send_push_notification(
            session_id=session.session_id,
            text=text,
            title=title,
        )
        if not sent:
            logger.warning(
                "[XiaoYi Adapter] proactive push failed for session %s",
                session.session_id,
            )
            return

        logger.info(
            "[XiaoYi Adapter] proactive push sent: session=%s text_len=%s",
            session.session_id,
            len(text),
        )
        await super().send_by_session(session, message_chain)

    async def shutdown(self):
        global _ACTIVE_XIAOYI_CLIENT
        self._running = False
        current_client = self.client
        if self.client:
            await self.client.stop()
            self.client = None
        if _ACTIVE_XIAOYI_CLIENT is current_client:
            _ACTIVE_XIAOYI_CLIENT = None

    async def terminate(self):
        await self.shutdown()

    async def run(self):
        self._running = True

        async def on_received(data: dict[str, Any]) -> None:
            await self._handle_payload(data)

        self.client = XiaoYiClient(
            ws_url=self.config.get("wsUrl", DEFAULT_WS_URL),
            ws_url2=self.config.get("wsUrl2") or None,
            ak=self.config["ak"],
            sk=self.config["sk"],
            agent_id=self.config["agentId"],
            api_id=self.config.get("apiId") or None,
            push_id=self.config.get("pushId") or None,
            push_url=self.config.get("pushUrl") or None,
            session_cleanup_delay_ms=int(self.config.get("sessionCleanupDelayMs", 300000)),
            session_state_ttl_ms=int(self.config.get("sessionStateTtlMs", 3600000)),
            on_message=on_received,
            logger=logger,
        )
        global _ACTIVE_XIAOYI_CLIENT
        _ACTIVE_XIAOYI_CLIENT = self.client

        try:
            await self.client.run_forever()
        finally:
            if _ACTIVE_XIAOYI_CLIENT is self.client:
                _ACTIVE_XIAOYI_CLIENT = None
            self._running = False

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        method = payload.get("method")

        if method in {"clearContext", "clear_context"}:
            session_id = payload.get("params", {}).get("sessionId") or payload.get("sessionId")
            if session_id and self.client:
                await self.client.send_clear_context_response(session_id, payload["id"])
                self.client.mark_session_for_cleanup(session_id)
                await _reset_astrbot_session(session_id, self.meta().id or "xiaoyi")
            return

        if method in {"tasks/cancel", "tasks_cancel"}:
            params = payload.get("params", {})
            session_id = params.get("sessionId") or payload.get("sessionId")
            task_id = params.get("id") or payload.get("taskId") or payload["id"]
            if session_id and self.client:
                await self.client.send_cancel_response(session_id, task_id, payload["id"])
            return

        if method != "message/stream":
            logger.warning("Ignore unsupported XiaoYi message method: %s", method)
            return

        abm = self.convert_message(payload)
        if self.client and self.config.get("sendProcessingStatus", True):
            await self.client.send_status_update(
                session_id=abm.session_id,
                task_id=payload["params"]["id"],
                request_id=payload["id"],
                text="任务正在处理中，请稍后~",
                state="working",
            )
        await self.handle_msg(abm)

    def convert_message(self, data: dict[str, Any]) -> AstrBotMessage:
        params = data.get("params", {})
        message = params.get("message", {})
        parts = message.get("parts", [])
        session_id = params.get("sessionId") or data.get("sessionId")

        abm = AstrBotMessage()
        abm.type = MessageType.FRIEND_MESSAGE
        abm.group_id = ""
        abm.raw_message = data
        abm.self_id = data.get("agentId", self.config.get("agentId", ""))
        abm.session_id = session_id
        abm.message_id = data.get("id", params.get("id", ""))
        abm.sender = MessageMember(user_id=session_id, nickname=session_id)

        message_chain = []
        text_fragments: list[str] = []
        raw_xiaoyi_parts: list[dict[str, Any]] = []

        for part in parts:
            kind = part.get("kind")
            raw_xiaoyi_parts.append(part)
            if kind == "text":
                text_value = part.get("text", "")
                if text_value:
                    text_fragments.append(text_value)
                    message_chain.append(Plain(text_value))
            elif kind == "file":
                file_info = part.get("file") or {}
                mime_type = file_info.get("mimeType", "")
                uri = file_info.get("uri")
                name = file_info.get("name", "file")
                if uri and mime_type.startswith("image/"):
                    message_chain.append(Image.fromURL(uri))
                else:
                    message_chain.append(Plain(f"[File:{name}]"))
            elif kind in {"data", "reasoningText", "command"}:
                continue

        if not message_chain:
            message_chain = [Plain("")]

        abm.message = message_chain
        abm.message_str = "\n".join(text_fragments).strip() if text_fragments else ""
        abm.raw_xiaoyi_parts = raw_xiaoyi_parts
        return abm

    async def handle_msg(self, message: AstrBotMessage):
        if not self.client:
            return
        event = XiaoYiAstrBotEvent(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.meta(),
            session_id=message.session_id,
            client=self.client,
            raw_payload=message.raw_message,
            stream_finalize_delay_ms=max(5000, int(self.config.get("streamFinalizeDelayMs", 5000))),
            push_enabled=bool(self.config.get("pushEnabled", False)),
            push_default_mode=str(self.config.get("pushDefaultMode", "push_only_for_async")),
            push_on_final=bool(self.config.get("pushOnFinal", False)),
        )
        self.commit_event(event)

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api import star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.platform.message_session import MessageSession


UNSUPPORTED_MESSAGE_NOTICE = "尚未支持该类型消息"
_ACTIVE_PLUGIN_CONTEXT: Context | None = None


def get_active_plugin_context() -> Context | None:
    return _ACTIVE_PLUGIN_CONTEXT


@star.register(
    "astrbot_plugin_xiaoyi_adapter",
    "QXAzusa, GPT",
    "将小艺 OpenClaw 类型通道接入 AstrBot 平台适配器体系",
    "0.0.4",
)
class XiaoYiPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        global _ACTIVE_PLUGIN_CONTEXT
        _ACTIVE_PLUGIN_CONTEXT = context
        logger.info("XiaoYiPlugin initialized; result hooks are being registered")
        try:
            from . import xiaoyi_astrbot_adapter  # noqa: F401
        except ImportError as exc:
            logger.error(f"Failed to import XiaoYi AstrBot adapter: {exc}")
            raise

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self):
        logger.info("XiaoYiPlugin on_astrbot_loaded fired; hooks should now be active")

    @filter.command("pushid")
    async def show_push_id(self, event: AstrMessageEvent):
        """输出当前会话缓存的 push_id。"""
        from .xiaoyi_astrbot_adapter import get_active_xiaoyi_client

        client = get_active_xiaoyi_client()
        if not client:
            yield event.plain_result("XiaoYi client 未运行，暂时无法读取 push_id。")
            return

        session_id = event.session_id
        if not session_id:
            try:
                session = MessageSession.from_str(event.unified_msg_origin)
                session_id = session.session_id
            except Exception:
                session_id = ""

        if not session_id:
            yield event.plain_result("当前会话缺少 session_id，无法读取 push_id。")
            return

        push_id = client.get_push_id(session_id)
        if not push_id:
            yield event.plain_result(
                f"当前会话 `{session_id}` 暂未缓存 push_id，请先确认小艺开发后台已启用该系统变量。"
            )
            return

        yield event.plain_result(f"session_id: {session_id}\npush_id: {push_id}")

    @filter.on_decorating_result(priority=100)
    async def normalize_xiaoyi_result(self, event: AstrMessageEvent):
        if event.get_platform_name() != "xiaoyi":
            return

        result = event.get_result()
        if result is None or not result.chain:
            return

        plain_components: list[Comp.Plain] = []
        ignored_components: list[str] = []

        for comp in result.chain:
            if isinstance(comp, Comp.Plain):
                if comp.text and comp.text.strip():
                    plain_components.append(comp)
                continue
            ignored_components.append(type(comp).__name__)

        if not ignored_components:
            return

        if plain_components:
            result.chain = plain_components
            logger.info(
                "XiaoYi decorating result stripped non-text components: sender=%s ignored=%s",
                event.get_sender_id(),
                ignored_components,
            )
            return

        result.chain = [Comp.Plain(UNSUPPORTED_MESSAGE_NOTICE)]
        logger.info(
            "XiaoYi decorating result replaced non-text-only reply: sender=%s ignored=%s",
            event.get_sender_id(),
            ignored_components,
        )

    @filter.after_message_sent(priority=100)
    async def fallback_xiaoyi_empty_result(self, event: AstrMessageEvent):
        if event.get_platform_name() != "xiaoyi":
            return
        if event.get_extra("_xiaoyi_fallback_sent", False):
            return

        result = event.get_result()
        if result is None:
            return

        chain = result.chain or []
        if chain:
            return

        logger.info(
            "XiaoYi after_message_sent fallback triggered for empty result: sender=%s",
            event.get_sender_id(),
        )
        event.set_extra("_xiaoyi_fallback_sent", True)
        await event.send(event.plain_result(UNSUPPORTED_MESSAGE_NOTICE))

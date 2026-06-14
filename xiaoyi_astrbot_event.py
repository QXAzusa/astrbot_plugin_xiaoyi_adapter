import asyncio
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import Image, Plain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata

from .xiaoyi_client import XiaoYiClient


UNSUPPORTED_MESSAGE_NOTICE = "尚未支持该类型消息"

class XiaoYiAstrBotEvent(AstrMessageEvent):
    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        client: XiaoYiClient,
        raw_payload: dict[str, Any],
        stream_finalize_delay_ms: int = 800,
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.client = client
        self.raw_payload = raw_payload
        self.stream_finalize_delay_ms = stream_finalize_delay_ms
        self._finalize_task: asyncio.Task | None = None
        self._final_sent = False
        self._accumulated_text = ""
        self._latest_task_id: str | None = None
        self._latest_request_id: str | None = None

    def get_message_outline(self) -> str:
        if not self.message_obj or not self.message_obj.message:
            return ""

        chain = self.message_obj.message
        iterable_chain = chain.chain if isinstance(chain, MessageChain) else chain
        outline: list[str] = []
        for item in iterable_chain:
            if isinstance(item, Plain):
                outline.append(item.text)
            elif isinstance(item, Image):
                outline.append("[Image]")
            else:
                outline.append(f"[{item.__class__.__name__}]")
        return " ".join(part for part in outline if part).strip()

    def _resolve_ids(self) -> tuple[str, str]:
        params = self.raw_payload.get("params", {})
        task_id = params.get("id") or self.raw_payload.get("id") or self.session_id
        request_id = self.raw_payload.get("id") or task_id
        return task_id, request_id

    async def _schedule_finalize(self, task_id: str, request_id: str) -> None:
        try:
            await asyncio.sleep(self.stream_finalize_delay_ms / 1000)
            if self._final_sent:
                return
            final_parts: list[dict[str, Any]] = []
            if self._accumulated_text:
                final_parts.append({"kind": "text", "text": self._accumulated_text})

            await self.client.send_status_update(
                session_id=self.session_id,
                task_id=task_id,
                request_id=request_id,
                text="任务处理已完成~",
                state="completed",
            )
            if final_parts:
                await self.client.send_artifact_update(
                    session_id=self.session_id,
                    task_id=task_id,
                    request_id=request_id,
                    parts=final_parts,
                    append=False,
                    final=True,
                )
                self._final_sent = True
            else:
                await self.client.send_artifact_update(
                    session_id=self.session_id,
                    task_id=task_id,
                    request_id=request_id,
                    parts=[{"kind": "text", "text": ""}],
                    append=False,
                    final=True,
                )
                self._final_sent = True
        except asyncio.CancelledError:
            raise

    async def send(self, message: MessageChain):
        if self._final_sent:
            logger.warning(
                "XiaoYi task %s for session %s already finalized; ignore late send() chunk",
                self._latest_task_id or self.session_id,
                self.session_id,
            )
            return
        self._has_send_oper = True

        chain = message.chain if isinstance(message, MessageChain) else message
        plain_parts: list[str] = []
        ignored_non_text_found = False

        for item in chain:
            if isinstance(item, Plain):
                if item.text:
                    plain_parts.append(item.text)
                continue

            if type(item) is Image:
                logger.info(
                    "XiaoYi outbound ignored Image component: session=%s task=%s item=%s",
                    self.session_id,
                    self._latest_task_id or self.session_id,
                    item,
                )
                ignored_non_text_found = True
                continue

            if hasattr(item, "reasoning_text") and isinstance(item.reasoning_text, str):
                continue

            if hasattr(item, "text") and isinstance(getattr(item, "text"), str):
                plain_parts.append(getattr(item, "text"))
                continue

            ignored_non_text_found = True
            logger.info(
                "XiaoYi outbound ignored non-text component: session=%s task=%s type=%s",
                self.session_id,
                self._latest_task_id or self.session_id,
                item.__class__.__name__,
            )

        if ignored_non_text_found and not plain_parts:
            logger.info(
                "XiaoYi outbound ignored non-text-only reply: session=%s",
                self.session_id,
            )
            plain_parts = [UNSUPPORTED_MESSAGE_NOTICE]

        raw_text = "".join(plain_parts)
        text = raw_text if raw_text.strip() else ""
        logger.info(
            "XiaoYi outbound send prepared: session=%s text_len=%s ignored_non_text=%s",
            self.session_id,
            len(text),
            ignored_non_text_found,
        )
        if text:
            self._accumulated_text += text

        task_id, request_id = self._resolve_ids()
        self._latest_task_id = task_id
        self._latest_request_id = request_id

        if self._finalize_task:
            self._finalize_task.cancel()
            try:
                await self._finalize_task
            except asyncio.CancelledError:
                pass

        self._finalize_task = asyncio.create_task(self._schedule_finalize(task_id, request_id))

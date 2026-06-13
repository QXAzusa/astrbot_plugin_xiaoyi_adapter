import asyncio
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import Image, Plain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata

from .xiaoyi_client import XiaoYiClient


def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as file_obj:
        return file_obj.read()


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
        push_enabled: bool = False,
        push_default_mode: str = "push_only_for_async",
        push_on_final: bool = False,
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.client = client
        self.raw_payload = raw_payload
        self.stream_finalize_delay_ms = stream_finalize_delay_ms
        self.push_enabled = push_enabled
        self.push_default_mode = push_default_mode
        self.push_on_final = push_on_final
        self._finalize_task: asyncio.Task | None = None
        self._final_sent = False
        self._push_summary_text = ""
        self._accumulated_text = ""
        self._pending_extra_parts: list[dict[str, Any]] = []
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
            final_parts.extend(self._pending_extra_parts)

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
                if (
                    self.push_enabled
                    and self.push_default_mode == "websocket_first"
                    and self.push_on_final
                    and self.client.is_push_configured()
                    and self._push_summary_text
                ):
                    await self.client.send_push_notification(
                        session_id=self.session_id,
                        text=self._push_summary_text,
                        title=self._push_summary_text.splitlines()[0][:57],
                    )
                self._pending_extra_parts = []
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

        chain = message.chain if isinstance(message, MessageChain) else message
        plain_parts: list[str] = []
        extra_parts: list[dict[str, Any]] = []

        for item in chain:
            if isinstance(item, Plain):
                if item.text:
                    plain_parts.append(item.text)
                continue

            if isinstance(item, Image):
                if getattr(item, "url", None):
                    extra_parts.append(
                        self.client.build_file_part_from_url(
                            item.url,
                            mime_type="image/png",
                            file_name=Path(item.url.split("?", 1)[0]).name or "image.png",
                        )
                    )
                elif getattr(item, "path", None):
                    try:
                        raw = await asyncio.to_thread(_read_file_bytes, item.path)
                    except Exception as exc:
                        logger.error(f"Failed to read local image for XiaoYi reply: {exc}")
                    else:
                        extra_parts.append(
                            self.client.build_file_part_from_bytes(
                                raw,
                                file_name=Path(item.path).name or "image.png",
                                mime_type="image/png",
                            )
                        )
                elif getattr(item, "raw", None):
                    extra_parts.append(
                        self.client.build_file_part_from_bytes(
                            item.raw,
                            file_name="image.png",
                            mime_type="image/png",
                        )
                    )
                continue

            if hasattr(item, "reasoning_text") and isinstance(item.reasoning_text, str):
                extra_parts.append(self.client.build_reasoning_text_part(item.reasoning_text))
                continue

            if hasattr(item, "command"):
                command = getattr(item, "command")
                if isinstance(command, dict):
                    extra_parts.append(self.client.build_command_part(command))
                    continue

            if hasattr(item, "data"):
                data = getattr(item, "data")
                if isinstance(data, dict):
                    extra_parts.append(self.client.build_data_part(data))
                    continue

            if hasattr(item, "text") and isinstance(getattr(item, "text"), str):
                plain_parts.append(getattr(item, "text"))
                continue

        raw_text = "".join(plain_parts)
        text = raw_text if raw_text.strip() else ""
        if text:
            self._accumulated_text += text
            self._push_summary_text = self._accumulated_text

        if extra_parts:
            self._pending_extra_parts.extend(extra_parts)

        if not self._accumulated_text and not self._pending_extra_parts:
            self._accumulated_text = (
                "[AstrBot returned a non-text response that this XiaoYi adapter could not serialize.]"
            )
            self._push_summary_text = self._accumulated_text

        task_id, request_id = self._resolve_ids()
        self._latest_task_id = task_id
        self._latest_request_id = request_id

        if self._finalize_task:
            self._finalize_task.cancel()
            try:
                await self._finalize_task
            except asyncio.CancelledError:
                pass

        if text:
            await self.client.send_stream_text(
                session_id=self.session_id,
                task_id=task_id,
                request_id=request_id,
                text=text,
                append=True,
            )
        self._finalize_task = asyncio.create_task(self._schedule_finalize(task_id, request_id))

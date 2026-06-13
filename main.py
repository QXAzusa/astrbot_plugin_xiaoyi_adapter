from astrbot.api import logger
from astrbot.api.star import Context, Star


class XiaoYiPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        try:
            from . import xiaoyi_astrbot_adapter  # noqa: F401
        except ImportError as exc:
            logger.error(f"Failed to import XiaoYi AstrBot adapter: {exc}")
            raise


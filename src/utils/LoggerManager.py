import sys
from loguru import logger

logger.remove()

logger.level("DEBUG", color="<blue>", icon="*️⃣DDDEBUG")
logger.level("INFO", color="<white>", icon="ℹ️IIIINFO")
logger.level("SUCCESS", color="<green>", icon="✅SUCCESS")
logger.level("WARNING", color="<yellow>", icon="⚠️WARNING")
logger.level("ERROR", color="<red>", icon="⭕EEERROR")

default_format = "<g>{time:MM-DD HH:mm:ss}</g> [{level.icon}] {message}"
logger_id = logger.add(sys.stdout, level="INFO", format=default_format, diagnose=False)

__all__ = ["logger"]
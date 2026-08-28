import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from src.translate.translator import translate
from src.utils.LoggerManager import logger


@dataclass
class TranslationJob:
    news_id: str
    revision: int
    message_id: int
    original_text: str
    source_time: str
    prefix: str
    apply_translation: Callable[[str, int, int, str], Awaitable[bool]]
    finish_attempt: Optional[Callable[[str, int, int, bool], None]] = None


class TranslationQueueWorker:
    def __init__(self, max_pending: int = 200):
        self._queue: asyncio.Queue[Optional[TranslationJob]] = asyncio.Queue(maxsize=max_pending)
        self._task: Optional[asyncio.Task[None]] = None
        self._dropped = 0

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="translate-worker")
        logger.info("🌐 Translation worker started")

    async def stop(self) -> None:
        if self._task is None:
            return
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            pass
        try:
            await asyncio.wait_for(self._task, timeout=10)
        except asyncio.TimeoutError:
            self._task.cancel()
        except Exception:
            pass
        self._task = None
        logger.info("🌐 Translation worker stopped")

    def submit(self, job: TranslationJob) -> bool:
        try:
            self._queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            self._dropped += 1
            logger.warning(f"Translation queue full, dropping job (total dropped: {self._dropped})")
            return False

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            if job is None:
                return
            try:
                await self._handle(job)
            except Exception as e:
                logger.error(f"Translation worker handler error: {type(e).__name__}: {e}")

    async def _handle(self, job: TranslationJob) -> None:
        applied = False
        try:
            translated = await translate(job.original_text)
            if not translated:
                logger.info(f"Skip edit (no translation) for msg_id={job.message_id}")
                return
            plain_text = _render_bilingual_text(
                prefix=job.prefix,
                original=job.original_text,
                translated=translated,
                source_time=job.source_time,
            )
            applied = await job.apply_translation(
                job.news_id, job.revision, job.message_id, plain_text
            )
            if applied:
                logger.info(
                    f"✏️  Published translation for news_id={job.news_id} revision={job.revision}"
                )
            else:
                logger.info(
                    f"Skip stale or failed translation for news_id={job.news_id} revision={job.revision}"
                )
        finally:
            if job.finish_attempt is not None:
                job.finish_attempt(job.news_id, job.revision, job.message_id, applied)


def _render_bilingual_text(prefix: str, original: str, translated: str, source_time: str) -> str:
    return (
        f"{prefix}{original}\n"
        f"———\n"
        f"{translated}\n\n"
        f"Source time: {source_time}"
    )

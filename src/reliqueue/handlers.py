from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def send_email_handler(_: dict[str, Any]) -> None:
    await asyncio.sleep(0.01)


async def report_handler(_: dict[str, Any]) -> None:
    await asyncio.sleep(0.02)


DEFAULT_HANDLERS: dict[str, Handler] = {
    "email.send": send_email_handler,
    "report.generate": report_handler,
}

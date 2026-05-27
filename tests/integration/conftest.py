from __future__ import annotations

import os

import asyncpg
import pytest

from reliqueue.db import create_pool, init_db


@pytest.fixture(scope="session")
def database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not set")
    return value


@pytest.fixture
async def db_pool(database_url: str):
    pool = await create_pool(database_url)
    await init_db(pool)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE dead_letter_jobs, jobs")
    try:
        yield pool
    finally:
        await pool.close()

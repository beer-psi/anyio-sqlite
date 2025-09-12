import contextlib
import os
import sqlite3
import string
import time

import pytest

import anyio_sqlite

pytestmark = pytest.mark.anyio


@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS") is not None, reason="too slow")
async def test_cursor_iterate_128k_rows():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.executescript("""
            CREATE TABLE ic_perf(
                i INTEGER PRIMARY KEY,
                k INTEGER,
                a INTEGER,
                b INTEGER,
                c TEXT
            )
        """)
        for batch in range(128):
            r_start = batch * 1024

            await conn.executemany(
                "INSERT INTO ic_perf (k, a, b, c) VALUES (?, 1, 2, ?)",
                [(i, string.ascii_lowercase) for i in range(r_start, r_start + 1024)],
            )
            await conn.commit()

        async with await conn.execute("SELECT * FROM ic_perf") as cursor:
            anyio_start_time = time.perf_counter_ns()
            anyio_rows = 0

            async for _ in cursor:
                anyio_rows += 1

            anyio_total_time = time.perf_counter_ns() - anyio_start_time

    with sqlite3.connect(":memory:") as conn:
        conn.executescript("""
            CREATE TABLE ic_perf(
                i INTEGER PRIMARY KEY,
                k INTEGER,
                a INTEGER,
                b INTEGER,
                c TEXT
            )
        """)
        for batch in range(128):
            r_start = batch * 1024

            conn.executemany(
                "INSERT INTO ic_perf (k, a, b, c) VALUES (?, 1, 2, ?)",
                [(i, string.ascii_lowercase) for i in range(r_start, r_start + 1024)],
            )
            conn.commit()

        with contextlib.closing(conn.execute("SELECT * FROM ic_perf")) as cursor:
            native_start_time = time.perf_counter_ns()
            native_rows = 0

            for _ in cursor:
                native_rows += 1

            native_total_time = time.perf_counter_ns() - native_start_time

    assert anyio_rows == native_rows

    native_total_time_ms = native_total_time / 1e6
    anyio_total_time_ms = anyio_total_time / 1e6

    print(
        f"{anyio_total_time_ms:0.4f}ms/{native_total_time_ms:0.4f}ms: {anyio_total_time / native_total_time:0.2f}"  # noqa: E501
    )
    print(f"anyio_sqlite average time: {anyio_total_time_ms / anyio_rows:0.5f}ms/row")
    print(f"native average time: {native_total_time_ms / native_rows:0.5f}ms/row")

    # asyncio hovers around 2, and trio hovers around 3
    # aiosqlite hovers around 1.7 on this test from my testing
    assert anyio_total_time / native_total_time <= 3.5

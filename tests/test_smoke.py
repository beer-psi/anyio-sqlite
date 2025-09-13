import sqlite3
import sys
from pathlib import Path
from typing import Optional

import anyio
import pytest

import anyio_sqlite

pytestmark = pytest.mark.anyio


async def test_connection_open_and_close():
    conn = await anyio_sqlite.connect(":memory:")

    assert isinstance(conn, anyio_sqlite.Connection)
    await conn.aclose()


async def test_connection_context_manager():
    async with await anyio_sqlite.connect(":memory:") as conn:
        assert isinstance(conn, anyio_sqlite.Connection)


async def test_connect_error():
    with pytest.raises(anyio_sqlite.OperationalError, match="unable to open database"):
        _ = await anyio_sqlite.connect("/something/that/shouldnt/exist.db")


async def test_connection_properly_closes_on_error():
    def bad_connector():
        msg = "hehe"
        raise BaseException(msg)  # noqa: TRY002

    connection = anyio_sqlite.Connection(bad_connector, 64)

    with pytest.raises(BaseException, match="hehe"):
        async with await connection:
            pass

    assert not connection._connected  # pyright: ignore[reportPrivateUsage]


async def test_closed_connection():
    async with await anyio_sqlite.connect(":memory:") as conn:
        pass

    with pytest.raises(anyio_sqlite.ProgrammingError, match="no active connections"):
        await conn.execute("SELECT 1")


async def test_close_twice():
    conn = await anyio_sqlite.connect(":memory:")

    await conn.aclose()
    await conn.aclose()


async def test_multiple_connections(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"

    async with await anyio_sqlite.connect(db_path) as conn:
        await conn.executescript("CREATE TABLE t1(i INTEGER NOT NULL)")

    async def do_one_conn(i: int):
        async with (
            await anyio_sqlite.connect(db_path) as conn,
            await conn.execute("INSERT INTO t1 VALUES(?) RETURNING i", (i,)) as cursor,
        ):
            row = await cursor.fetchone()
            await conn.commit()

            assert row is not None
            assert row[0] == i

    async with anyio.create_task_group() as tg:
        for i in range(10):
            tg.start_soon(do_one_conn, i)

    async with (
        await anyio_sqlite.connect(db_path) as conn,
        await conn.execute("SELECT COUNT(*) FROM t1") as cursor,
    ):
        row = await cursor.fetchone()

        assert row is not None
        assert row[0] == 10


async def test_multiple_queries():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.executescript("CREATE TABLE t1(i INTEGER NOT NULL)")

        async with anyio.create_task_group() as tg:
            for i in range(10):
                tg.start_soon(conn.execute, "INSERT INTO t1 VALUES (?)", (i,))

            await conn.commit()

        async with await conn.execute("SELECT COUNT(*) FROM t1") as cursor:
            row = await cursor.fetchone()

            assert row is not None
            assert row[0] == 10


async def test_iterable_cursor():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.executescript("CREATE TABLE t1(i INTEGER NOT NULL)")
        await conn.executemany("INSERT INTO t1 VALUES (?)", [(i,) for i in range(10)])
        await conn.commit()

        rows: int = 0

        async with await conn.execute("SELECT * FROM t1") as cursor:
            async for _ in cursor:
                rows += 1

        assert rows == 10


async def test_cursor_returns_self():
    async with await anyio_sqlite.connect(":memory:") as conn:
        cursor = await conn.cursor()
        cursor2 = await cursor.execute("SELECT 1")

        assert cursor is cursor2


async def test_create_function():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.create_function("fn", 1, lambda x: 42)  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]

        async with await conn.execute("SELECT fn(1)") as cursor:
            row = await cursor.fetchone()
            assert row == (42,)

        with pytest.raises(anyio_sqlite.OperationalError):
            await conn.executescript("""
                CREATE TABLE t1(i INTEGER PRIMARY KEY);
                CREATE INDEX idx_t1 ON t1(fn(i));
            """)


async def test_create_function_deterministic():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.create_function("fn", 1, lambda x: 42, deterministic=True)  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]

        async with await conn.execute("SELECT fn(1)") as cursor:
            row = await cursor.fetchone()
            assert row == (42,)

        await conn.executescript("""
            CREATE TABLE t1(i INTEGER PRIMARY KEY);
            CREATE INDEX idx_t1 ON t1(fn(i));
        """)


async def test_create_collation():
    def collate_reverse(string1: str, string2: str):
        if string1 == string2:
            return 0
        if string1 < string2:
            return 1
        return -1

    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.create_collation("reverse", collate_reverse)

        cursor = await conn.execute("CREATE TABLE test(x)")

        await cursor.executemany("INSERT INTO test(x) VALUES(?)", [("a",), ("b",)])
        await cursor.execute("SELECT x FROM test ORDER BY x COLLATE reverse")

        rows = await cursor.fetchall()

        assert rows == [("b",), ("a",)]


async def test_set_authorizer():
    def authorizer(
        op: int,
        arg1: Optional[str],
        arg2: Optional[str],
        db_name: Optional[str],
        trigger_or_view: Optional[str],
    ) -> int:
        return anyio_sqlite.SQLITE_DENY

    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.set_authorizer(authorizer)

        with pytest.raises(anyio_sqlite.DatabaseError, match="not authorized"):
            await conn.execute("SELECT 1")


async def test_set_progress_handler():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.set_progress_handler(lambda: 1, 1)

        with pytest.raises(anyio_sqlite.OperationalError):
            await conn.execute("SELECT 1")


async def test_set_trace_callback():
    class Tracer:
        def __init__(self):
            self.statements: list[str] = []

        def __call__(self, statement: str):
            self.statements.append(statement)

    tracer = Tracer()
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.set_trace_callback(tracer)
        await conn.execute("SELECT 1")
        await conn.execute("SELECT 2")
        await conn.execute("SELECT 3")

        assert tracer.statements == ["SELECT 1", "SELECT 2", "SELECT 3"]


@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="cannot test loading extensions if python was not compiled with sqlite3 extension support",  # noqa: E501
)
async def test_load_extension():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.enable_load_extension(True)

        with pytest.raises(
            anyio_sqlite.OperationalError,
            check=lambda e: "not authorized" not in e.args,
        ):
            await conn.load_extension("test")


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="getlimit and setlimit were added in Python 3.11",
)
async def test_getlimit_setlimit():
    if sys.version_info >= (3, 11):
        async with await anyio_sqlite.connect(":memory:") as conn:
            await conn.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 1)
            assert await conn.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH) == 1

            with pytest.raises(
                anyio_sqlite.DataError, match="query string is too large"
            ):
                await conn.execute("SELECT 1")


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="serialize and deserialize were added in Python 3.11",
)
async def test_serialize_deserialize():
    if sys.version_info >= (3, 11):
        async with (
            await anyio_sqlite.connect(":memory:") as conn1,
            await anyio_sqlite.connect(":memory:") as conn2,
        ):
            await conn1.executescript("""
                CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);
                INSERT INTO t1 (k) VALUES ('hello');
                INSERT INTO t1 (k) VALUES ('world');
            """)
            await conn1.commit()

            with pytest.raises(
                anyio_sqlite.OperationalError, match="no such table: t1"
            ):
                await conn2.execute("SELECT * FROM t1")

            await conn2.deserialize(await conn1.serialize())

            async with await conn2.execute("SELECT * FROM t1") as cursor:
                rows = await cursor.fetchall()
                assert rows == [(1, "hello"), (2, "world")]


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="getconfig and setconfig were added in Python 3.12",
)
async def test_getconfig_setconfig():
    if sys.version_info >= (3, 12):
        async with await anyio_sqlite.connect(":memory:") as conn:
            await conn.executescript("CREATE TABLE t1(t TEXT);")

            assert await conn.getconfig(anyio_sqlite.SQLITE_DBCONFIG_DQS_DML)
            await conn.execute('INSERT INTO t1 VALUES ("test")')
            await conn.commit()

            await conn.setconfig(anyio_sqlite.SQLITE_DBCONFIG_DQS_DML, False)
            assert not await conn.getconfig(anyio_sqlite.SQLITE_DBCONFIG_DQS_DML)
            with pytest.raises(
                anyio_sqlite.OperationalError,
                match=r'no such column: "?test"?.*',
            ):
                await conn.execute('INSERT INTO t1 VALUES ("test")')


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="autocommit were added in Python 3.12",
)
async def test_autocommit():
    if sys.version_info >= (3, 12):
        async with await anyio_sqlite.connect(":memory:") as conn:
            assert await conn.autocommit() == anyio_sqlite.LEGACY_TRANSACTION_CONTROL
            await conn.set_autocommit(False)
            assert not await conn.autocommit()

            with pytest.raises(
                anyio_sqlite.ProgrammingError,
                match="SQLite objects created in a thread can only be used in that same thread",  # noqa: E501
            ):
                conn.connection.autocommit = False


async def test_iterdump():
    async with await anyio_sqlite.connect(":memory:") as conn:
        await conn.executescript("""
            CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);
            INSERT INTO t1 (k) VALUES ('hello');
            INSERT INTO t1 (k) VALUES ('world');
        """)
        await conn.commit()

        lines = [line async for line in conn.iterdump()]

        assert lines == [
            "BEGIN TRANSACTION;",
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);",
            "INSERT INTO \"t1\" VALUES(1,'hello');",
            "INSERT INTO \"t1\" VALUES(2,'world');",
            "COMMIT;",
        ]


@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="iterdump(filter) is only available in Python 3.13 and above",
)
async def test_iterdump_with_filter():
    if sys.version_info >= (3, 13):
        async with await anyio_sqlite.connect(":memory:") as conn:
            await conn.executescript("""
                CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);
                INSERT INTO t1 (k) VALUES ('hello');
                INSERT INTO t1 (k) VALUES ('world');
            """)
            await conn.commit()

            lines = [line async for line in conn.iterdump(filter="t2")]

            assert lines == ["BEGIN TRANSACTION;", "COMMIT;"]


async def test_backup_anyio_sqlite():
    async with (
        await anyio_sqlite.connect(":memory:") as conn1,
        await anyio_sqlite.connect(":memory:") as conn2,
    ):
        await conn1.executescript("""
            CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);
            INSERT INTO t1 (k) VALUES ('hello');
            INSERT INTO t1 (k) VALUES ('world');
        """)
        await conn1.commit()

        with pytest.raises(anyio_sqlite.OperationalError, match="no such table: t1"):
            await conn2.execute("SELECT * FROM t1")

        await conn1.backup(conn2)

        async with await conn2.execute("SELECT * FROM t1") as cursor:
            rows = await cursor.fetchall()
            assert rows == [(1, "hello"), (2, "world")]


async def test_backup_sqlite3():
    async with await anyio_sqlite.connect(":memory:") as conn1:
        with sqlite3.connect(":memory:") as conn2:
            await conn1.executescript("""
                CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT);
                INSERT INTO t1 (k) VALUES ('hello');
                INSERT INTO t1 (k) VALUES ('world');
            """)
            await conn1.commit()

            with pytest.raises(sqlite3.OperationalError, match="no such table: t1"):
                conn2.execute("SELECT * FROM t1")

            await conn1.backup(conn2)

            cursor = conn2.execute("SELECT * FROM t1")
            rows = cursor.fetchall()

            assert rows == [(1, "hello"), (2, "world")]
            cursor.close()


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="blobopen was added in Python 3.11"
)
async def test_blobopen():
    if sys.version_info >= (3, 11):
        async with await anyio_sqlite.connect(":memory:") as conn:
            await conn.execute("CREATE TABLE test(blob_col blob)")
            await conn.execute("INSERT INTO test(blob_col) VALUES(zeroblob(13))")

            async with await conn.blobopen("test", "blob_col", 1) as blob:
                assert await blob.tell() == 0
                assert await blob.length() == 13

                await blob.write(b"hello ")
                await blob.write(b"world")
                assert await blob.tell() == 11

                await blob.seek(6)
                assert await blob.tell() == 6
                assert await blob.read(5) == b"world"


async def test_commits_on_context_manager_exit(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"

    async with await anyio_sqlite.connect(db_path) as conn:
        await conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT)")
        await conn.execute("INSERT INTO t1 (k) VALUES ('a'), ('b')")

    async with (
        await anyio_sqlite.connect(db_path) as conn,
        await conn.execute("SELECT k FROM t1") as cursor,
    ):
        assert await cursor.fetchall() == [("a",), ("b",)]


async def test_rollbacks_on_context_manager_exit(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"

    with pytest.raises(RuntimeError, match="hehe"):
        async with await anyio_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, k TEXT)")
            await conn.execute("INSERT INTO t1 (k) VALUES ('a')")
            await conn.commit()

            await conn.execute("INSERT INTO t1 (k) VALUES ('b')")
            raise RuntimeError("hehe")  # noqa: EM101

    async with (
        await anyio_sqlite.connect(db_path) as conn,
        await conn.execute("SELECT k FROM t1") as cursor,
    ):
        assert await cursor.fetchall() == [("a",)]

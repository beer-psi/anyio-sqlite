import sqlite3
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self

    from .connection import Connection

SyncConnectionT = TypeVar("SyncConnectionT", bound=sqlite3.Connection)
SyncCursorT = TypeVar("SyncCursorT", bound=sqlite3.Cursor)


class Cursor(Generic[SyncConnectionT, SyncCursorT]):
    def __init__(
        self,
        connection: "Connection[SyncConnectionT]",
        cursor: "SyncCursorT",
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self.iter_chunk_size = connection.iter_chunk_size

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional["TracebackType"],
    ):
        await self.aclose()

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iterator()

    async def _iterator(self):
        while True:
            rows = await self.fetchmany(self.iter_chunk_size)

            if not rows:
                break

            for row in rows:
                yield row

    async def execute(
        self, sql: str, parameters: Union[Sequence[Any], Mapping[str, Any]] = (), /
    ):
        await self._connection._to_thread(self._cursor.execute, sql, parameters)
        return self

    async def executemany(
        self, sql: str, parameters: Iterable[Union[Sequence[Any], Mapping[str, Any]]], /
    ):
        await self._connection._to_thread(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str, /) -> "Self":
        await self._connection._to_thread(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> Any:
        return await self._connection._to_thread(self._cursor.fetchone)

    async def fetchmany(self, size: Optional[int] = None) -> list[Any]:
        if size is None:
            size = self.arraysize

        return await self._connection._to_thread(self._cursor.fetchmany, size)

    async def fetchall(self) -> list[Any]:
        return await self._connection._to_thread(self._cursor.fetchall)

    async def aclose(self):
        return await self._connection._to_thread(self._cursor.close)

    async def setinputsizes(self, sizes: object, /):
        return self._connection._to_thread(self._cursor.setinputsizes, sizes)

    async def setoutputsize(self, size: object, column: object = None, /):
        return self._connection._to_thread(self._cursor.setoutputsize, size, column)

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        self._cursor.arraysize = value

    @property
    def connection(self) -> "Connection[SyncConnectionT]":
        """
        Read-only attribute that provides the database connection belonging to
        the cursor.
        """
        return self._connection

    @property
    def description(self):
        return self._cursor.description

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def row_factory(self):
        return self._cursor.row_factory

    @row_factory.setter
    def row_factory(
        self, value: Optional[Callable[[sqlite3.Cursor, sqlite3.Row], object]]
    ):
        self._cursor.row_factory = value

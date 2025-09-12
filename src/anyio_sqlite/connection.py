# pyright: reportPrivateUsage=false
import asyncio
import math
import sqlite3
import sys
from collections.abc import (
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Mapping,
    Sequence,
)
from functools import partial
from os import PathLike
from queue import SimpleQueue
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Optional,
    SupportsIndex,
    TypeVar,
    Union,
)

import anyio
import anyio.from_thread
import anyio.lowlevel
import anyio.to_thread
from aioresult import Future, TaskFailedException
from typing_extensions import ParamSpec, Self

from ._types import StopRunning
from .cursor import Cursor

if sys.version_info >= (3, 11):
    from .blob import Blob

if TYPE_CHECKING:
    from types import TracebackType

    from _typeshed import ReadableBuffer

    from ._types import (
        AggregateProtocol,
        IsolationLevel,
        SqliteData,
        WindowAggregateClass,
    )
    from .cursor import SyncCursorT

ArgsT = ParamSpec("ArgsT")
ReturnT = TypeVar("ReturnT")
SyncConnectionT = TypeVar("SyncConnectionT", bound=sqlite3.Connection)


class Connection(Generic[SyncConnectionT]):
    """
    An asynchronous SQLite database connection.
    Create a connection using anyio_sqlite.connect().
    """

    def __init__(self, connector: Callable[[], SyncConnectionT], iter_chunk_size: int):
        super().__init__()

        self.iter_chunk_size = iter_chunk_size

        self._connected: bool = False
        self._closed: bool = False

        self._connection: Optional[SyncConnectionT] = None
        self._connector = connector

        self._tg = anyio.create_task_group()
        self._tx: SimpleQueue[
            Union[tuple[object, Future[Any], Callable[[], Any]], StopRunning]
        ] = SimpleQueue()
        self._iterdump_send, self._iterdump_recv = anyio.create_memory_object_stream[
            Optional[str]
        ](math.inf)

    @property
    def connection(self) -> SyncConnectionT:
        """
        Returns the underlying SQLite connection. Raises sqlite3.ProgrammingError
        if there are none.
        """

        if self._connection is None:
            msg = "no active connections"
            raise sqlite3.ProgrammingError(msg)

        return self._connection

    def _worker_thread(self):
        while True:
            request = self._tx.get()

            if isinstance(request, StopRunning):
                break

            token, future, fn = request

            if isinstance(token, asyncio.AbstractEventLoop):
                run_sync_soon_from_thread = token.call_soon_threadsafe
            elif (run_sync_soon := getattr(token, "run_sync_soon", None)) is not None:
                run_sync_soon_from_thread = run_sync_soon
            else:
                run_sync_soon_from_thread = anyio.from_thread.run_sync

            try:
                try:
                    result = fn()
                except BaseException as e:  # noqa: BLE001
                    run_sync_soon_from_thread(future.set_exception, e)
                else:
                    run_sync_soon_from_thread(future.set_result, result)
            except RuntimeError:  # the event loop got closed
                break

    # Lifecycle management

    async def __aenter__(self) -> "Self":
        if self._closed:
            msg = "Cannot operate on a closed database."
            raise sqlite3.ProgrammingError(msg)

        return await self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional["TracebackType"],
    ):
        await self.aclose()

    async def _connect(self):
        if self._connected:
            return self

        tg = await self._tg.__aenter__()
        tg.start_soon(anyio.to_thread.run_sync, self._worker_thread)

        if self._connection is None:
            try:
                self._connection = await self._to_thread(self._connector)
            except BaseException:
                await self._stop_running()
                self._connection = None
                raise

        self._connected = True
        return self

    async def _stop_running(self):
        self._closed = True

        self._tx.put_nowait(StopRunning(anyio.lowlevel.current_token()))
        await self._iterdump_send.aclose()
        await self._iterdump_recv.aclose()

        await self._tg.__aexit__(None, None, None)

    async def aclose(self):
        """Closes the connection."""

        if self._connection is None:
            return

        try:
            await self._to_thread(self._connection.close)
        finally:
            await self._stop_running()
            self._connection = None

    async def _to_thread(
        self,
        func: Callable[ArgsT, ReturnT],
        *args: ArgsT.args,
        **kwargs: ArgsT.kwargs,
    ) -> ReturnT:
        await anyio.lowlevel.checkpoint_if_cancelled()

        if self._closed:
            msg = "Cannot operate on a closed database."
            raise sqlite3.ProgrammingError(msg)

        future = Future[ReturnT]()

        self._tx.put_nowait(
            (anyio.lowlevel.current_token(), future, partial(func, *args, **kwargs))
        )
        await future.wait_done()

        try:
            return future.result()
        except TaskFailedException:
            inner = future.exception()
            # if TaskFailedException is raised, then the exception is not None,
            # but pyright can't know that.
            raise inner from None  # pyright: ignore[reportGeneralTypeIssues]

    async def cursor(
        self, factory: type["SyncCursorT"] = sqlite3.Cursor
    ) -> Cursor[SyncConnectionT, "SyncCursorT"]:
        """
        Create and return a Cursor asynchronous proxy. The method accepts an optional
        parameter `factory`, for customizing the underlying `sqlite3` cursor class.
        """
        sync_cursor = await self._to_thread(self.connection.cursor, factory)

        return Cursor(self, sync_cursor)

    if sys.version_info >= (3, 11):

        async def blobopen(
            self,
            table: str,
            column: str,
            row: int,
            /,
            *,
            readonly: bool = False,
            name: str = "main",
        ):
            sync_blob = await self._to_thread(
                self.connection.blobopen,
                table,
                column,
                row,
                readonly=readonly,
                name=name,
            )

            return Blob(self, sync_blob)

    async def commit(self):
        await self._to_thread(self.connection.commit)

    async def rollback(self):
        await self._to_thread(self.connection.rollback)

    async def execute(
        self, sql: str, parameters: Union[Sequence[Any], Mapping[str, Any]] = (), /
    ) -> Cursor[SyncConnectionT, sqlite3.Cursor]:
        sync_cursor = await self._to_thread(self.connection.execute, sql, parameters)

        return Cursor(self, sync_cursor)

    async def executemany(
        self, sql: str, parameters: Iterable[Union[Sequence[Any], Mapping[str, Any]]], /
    ) -> Cursor[SyncConnectionT, sqlite3.Cursor]:
        sync_cursor = await self._to_thread(
            self.connection.executemany, sql, parameters
        )

        return Cursor(self, sync_cursor)

    async def executescript(
        self, sql_script: str, /
    ) -> Cursor[SyncConnectionT, sqlite3.Cursor]:
        sync_cursor = await self._to_thread(self.connection.executescript, sql_script)

        return Cursor(self, sync_cursor)

    async def create_function(
        self,
        name: str,
        narg: int,
        func: Optional[Callable[..., "SqliteData"]],
        *,
        deterministic: bool = False,
    ) -> None:
        return await self._to_thread(
            self.connection.create_function,
            name,
            narg,
            func,
            deterministic=deterministic,
        )

    async def create_aggregate(
        self, name: str, n_arg: int, aggregate_class: Callable[[], "AggregateProtocol"]
    ) -> None:
        return await self._to_thread(
            self.connection.create_aggregate,
            name,
            n_arg,
            aggregate_class,
        )

    if sys.version_info >= (3, 11):

        async def create_window_function(
            self,
            name: str,
            num_params: int,
            aggregate_class: Callable[[], "WindowAggregateClass"] | None,
            /,
        ) -> None:
            return await self._to_thread(
                self.connection.create_window_function,
                name,
                num_params,
                aggregate_class,
            )

    async def create_collation(
        self,
        name: str,
        callable: Optional[Callable[[str, str], Union[int, SupportsIndex]]],
        /,
    ) -> None:
        return await self._to_thread(
            self.connection.create_collation,
            name,
            callable,
        )

    def interrupt(self):
        self.connection.interrupt()

    async def set_authorizer(
        self,
        authorizer_callback: Optional[
            Callable[
                [int, Optional[str], Optional[str], Optional[str], Optional[str]], int
            ]
        ],
    ) -> None:
        return await self._to_thread(
            self.connection.set_authorizer,
            authorizer_callback,
        )

    async def set_progress_handler(
        self, progress_handler: Optional[Callable[[], Optional[int]]], n: int
    ) -> None:
        return await self._to_thread(
            self.connection.set_progress_handler,
            progress_handler,
            n,
        )

    async def set_trace_callback(
        self, trace_callback: Optional[Callable[[str], object]]
    ) -> None:
        return await self._to_thread(
            self.connection.set_trace_callback,
            trace_callback,
        )

    async def enable_load_extension(self, enabled: bool, /) -> None:
        return await self._to_thread(
            self.connection.enable_load_extension,
            enabled,
        )

    if sys.version_info >= (3, 12):

        async def load_extension(
            self, path: str, /, *, entrypoint: str | None = None
        ) -> None:
            return await self._to_thread(
                self.connection.load_extension,
                path,
                entrypoint=entrypoint,
            )

    else:

        async def load_extension(self, path: str, /) -> None:
            return await self._to_thread(
                self.connection.load_extension,
                path,
            )

    # TODO: change iterdumper to use anyio.from_thread.run(token=anyio_token)
    # as fallback when anyio 4.11 comes out
    if sys.version_info >= (3, 13):

        def _iterdumper(self, anyio_token: object, filter: str | None):
            if isinstance(anyio_token, asyncio.AbstractEventLoop):
                from_thread_run_sync_soon = anyio_token.call_soon_threadsafe
            elif (
                run_sync_soon := getattr(anyio_token, "run_sync_soon", None)
            ) is not None:
                from_thread_run_sync_soon = run_sync_soon
            else:
                msg = "Unsupported anyio token"
                raise ValueError(msg)

            try:
                for line in self.connection.iterdump(filter=filter):
                    from_thread_run_sync_soon(self._iterdump_send.send_nowait, line)
            finally:
                from_thread_run_sync_soon(self._iterdump_send.send_nowait, None)

        async def iterdump(self, *, filter: str | None = None) -> AsyncIterator[str]:
            self._tg.start_soon(
                lambda: self._to_thread(
                    self._iterdumper, anyio.lowlevel.current_token(), filter
                )
            )

            async for line in self._iterdump_recv:
                if line is None:
                    break

                yield line
    else:

        def _iterdumper(self, anyio_token: object):
            if isinstance(anyio_token, asyncio.AbstractEventLoop):
                from_thread_run_sync_soon = anyio_token.call_soon_threadsafe
            elif (
                run_sync_soon := getattr(anyio_token, "run_sync_soon", None)
            ) is not None:
                from_thread_run_sync_soon = run_sync_soon
            else:
                msg = "Unsupported anyio token"
                raise ValueError(msg)

            try:
                for line in self.connection.iterdump():
                    from_thread_run_sync_soon(self._iterdump_send.send_nowait, line)
            finally:
                from_thread_run_sync_soon(self._iterdump_send.send_nowait, None)

        async def iterdump(self) -> AsyncIterator[str]:
            self._tg.start_soon(
                lambda: self._to_thread(
                    self._iterdumper, anyio.lowlevel.current_token()
                )
            )

            async for line in self._iterdump_recv:
                if line is None:
                    break

                yield line

    async def backup(
        self,
        target: Union["Connection[SyncConnectionT]", sqlite3.Connection],
        *,
        pages: int = -1,
        progress: Optional[Callable[[int, int, int], Any]] = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        if isinstance(target, Connection):
            target = target.connection

        return await self._to_thread(
            self.connection.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )

    if sys.version_info >= (3, 11):

        async def getlimit(self, category: int, /) -> int:
            return await self._to_thread(self.connection.getlimit, category)

        async def setlimit(self, category: int, limit: int, /) -> int:
            return await self._to_thread(self.connection.setlimit, category, limit)

        async def serialize(self, *, name: str = "main") -> bytes:
            return await self._to_thread(self.connection.serialize, name=name)

        async def deserialize(
            self, data: "ReadableBuffer", /, *, name: str = "main"
        ) -> None:
            return await self._to_thread(self.connection.deserialize, data, name=name)

    if sys.version_info >= (3, 12):

        async def getconfig(self, op: int, /) -> bool:
            return await self._to_thread(self.connection.getconfig, op)

        async def setconfig(self, op: int, enable: bool = True, /) -> bool:
            return await self._to_thread(self.connection.setconfig, op, enable)

        async def autocommit(self) -> int:
            return await self._to_thread(getattr, self.connection, "autocommit")

        async def set_autocommit(self, val: int, /) -> None:
            def inner(connection: sqlite3.Connection, val: int):
                connection.autocommit = val

            return await self._to_thread(inner, self.connection, val)

    @property
    def in_transaction(self) -> bool:
        return self.connection.in_transaction

    @property
    def isolation_level(self) -> Optional[Union["IsolationLevel", Literal[""]]]:
        return self.connection.isolation_level

    @isolation_level.setter
    def isolation_level(self, val: Optional["IsolationLevel"]) -> None:
        self.connection.isolation_level = val

    @property
    def row_factory(self) -> Optional[Callable[[sqlite3.Cursor, sqlite3.Row], object]]:
        return self.connection.row_factory

    @row_factory.setter
    def row_factory(
        self, value: Optional[Callable[[sqlite3.Cursor, sqlite3.Row], object]]
    ):
        self.connection.row_factory = value

    @property
    def text_factory(self) -> Union[Callable[[bytes], str], str, bytes, bytearray]:
        return self.connection.text_factory

    @text_factory.setter
    def text_factory(self, value: Union[Callable[[bytes], str], str, bytes, bytearray]):
        self.connection.text_factory = value

    @property
    def total_changes(self):
        return self.connection.total_changes

    def __await__(self) -> Generator[Any, None, "Self"]:
        return self._connect().__await__()


if sys.version_info >= (3, 12):

    def connect(
        database: str | bytes | PathLike[str] | PathLike[bytes],
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"]
        | None = "DEFERRED",
        factory: type[SyncConnectionT] = sqlite3.Connection,
        cached_statements: int = 128,
        uri: bool = False,
        autocommit: bool = sqlite3.LEGACY_TRANSACTION_CONTROL,  # pyright: ignore[reportArgumentType]
        iter_chunk_size: int = 64,
    ) -> Connection[SyncConnectionT]:
        def connector():
            return sqlite3.connect(
                database,
                timeout=timeout,
                detect_types=detect_types,
                isolation_level=isolation_level,
                factory=factory,
                cached_statements=cached_statements,
                uri=uri,
                autocommit=autocommit,
            )

        return Connection(connector, iter_chunk_size)
else:

    def connect(
        database: Union[str, bytes, PathLike[str], PathLike[bytes]],
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: Optional[
            Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"]
        ] = "DEFERRED",
        factory: type[SyncConnectionT] = sqlite3.Connection,
        cached_statements: int = 128,
        uri: bool = False,
        iter_chunk_size: int = 64,
    ) -> Connection[SyncConnectionT]:
        def connector():
            return sqlite3.connect(
                database,
                timeout=timeout,
                detect_types=detect_types,
                isolation_level=isolation_level,
                factory=factory,
                cached_statements=cached_statements,
                uri=uri,
            )

        return Connection(connector, iter_chunk_size)

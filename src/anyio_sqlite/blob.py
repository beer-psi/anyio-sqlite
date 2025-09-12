import sys

if sys.version_info >= (3, 11):
    import sqlite3
    from typing import TYPE_CHECKING, Literal, Self, TypeVar, final

    if TYPE_CHECKING:
        from types import TracebackType

        from _typeshed import ReadableBuffer

        from .connection import Connection

    SyncConnectionT = TypeVar("SyncConnectionT", bound=sqlite3.Connection)

    @final
    class Blob:
        """
        An asynchronous blob object.

        This class wraps an sqlite3.Blob object and provides async friendly versions
        of the original blocking methods.

        To get the blob's length, call the length() method.

        Direct access to the blob's data using indices and slices is not supported.

        Use the blob as an asynchronous context manager to ensure that the blob handle
        is closed after use.
        """

        def __init__(
            self, connection: "Connection[SyncConnectionT]", blob: sqlite3.Blob
        ):
            self._connection = connection
            self._blob = blob

        async def aclose(self) -> None:
            """Close the blob."""
            return await self._connection._to_thread(self._blob.close)

        async def read(self, length: int = -1, /) -> bytes:
            return await self._connection._to_thread(self._blob.read, length)

        async def write(self, data: "ReadableBuffer", /) -> None:
            return await self._connection._to_thread(self._blob.write, data)

        async def tell(self) -> int:
            return await self._connection._to_thread(self._blob.tell)

        async def seek(self, offset: int, origin: int = 0, /) -> None:
            return await self._connection._to_thread(self._blob.seek, offset, origin)

        async def length(self) -> int:
            """Returns the length of the blob."""
            return await self._connection._to_thread(len, self._blob)

        async def __aenter__(self) -> Self:
            await self._connection._to_thread(self._blob.__enter__)
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: "TracebackType | None",
        ) -> Literal[False]:
            await self.aclose()
            return False

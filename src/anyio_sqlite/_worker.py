import queue
import threading
from typing import TYPE_CHECKING

import anyio.from_thread
from aioresult import Future

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any, TypeVar

    import anyio.lowlevel
    from typing_extensions import ParamSpec

    _T = TypeVar("_T")
    _ArgsT = ParamSpec("_ArgsT")
    _ReturnT = TypeVar("_ReturnT")


def _safe_set_result(future: Future["_T"], result: "_T"):
    if not future.is_done():
        future.set_result(result)


def _safe_set_exception(future: Future["_T"], exception: BaseException):
    if not future.is_done():
        future.set_exception(exception)


def run_sync_soon_from_thread(
    token: "anyio.lowlevel.EventLoopToken",
    fn: "Callable[_ArgsT, Any]",
    *args: "_ArgsT.args",
    **kwargs: "_ArgsT.kwargs",
):
    def wrapper():
        fn(*args, **kwargs)

    # Specializations for known backends because we do not care about the results,
    # and capturing results incur some overhead. This does not do much for Trio,
    # but there was a 20% decrease in time for AsyncIO in the tests/test_performance.py
    # benchmarks.
    if token.backend_class.__name__ == "AsyncIOBackend":
        # native_token is asyncio.AbstractEventLoop
        token.native_token.call_soon_threadsafe(wrapper)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    elif token.backend_class.__name__ == "TrioBackend":
        # native_token is trio.lowlevel.TrioToken
        token.native_token.run_sync_soon(wrapper)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    else:
        anyio.from_thread.run_sync(wrapper, token=token)


class _WorkerEntry:
    __slots__ = ("args", "fn", "future", "kwargs", "token")

    def __init__(
        self,
        token: "anyio.lowlevel.EventLoopToken",
        future: Future["Any"],
        fn: "Callable[..., Any]",
        args: tuple["Any", ...],
        kwargs: dict[str, "Any"],
    ):
        self.token = token
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class Worker:
    __slots__ = ("_end", "_worker_queue")

    def __init__(self) -> None:
        self._worker_queue: queue.Queue[_WorkerEntry] = queue.Queue()
        self._end: threading.Event = threading.Event()

    def _call_entry(self, entry: _WorkerEntry) -> None:
        try:
            result = entry.fn(*entry.args, **entry.kwargs)
        except BaseException as e:  # noqa: BLE001
            run_sync_soon_from_thread(entry.token, _safe_set_exception, entry.future, e)
        else:
            run_sync_soon_from_thread(
                entry.token, _safe_set_result, entry.future, result
            )

    def run(self):
        tx = self._worker_queue
        end = self._end

        while not end.is_set():
            try:
                entry = tx.get(timeout=0.2)
            except queue.Empty:  # noqa: PERF203
                continue
            else:
                self._call_entry(entry)

    def post(
        self,
        fn: "Callable[_ArgsT, _ReturnT]",
        *args: "_ArgsT.args",
        **kwargs: "_ArgsT.kwargs",
    ) -> Future["_ReturnT"]:
        future = Future["_ReturnT"]()

        self._worker_queue.put_nowait(
            _WorkerEntry(anyio.lowlevel.current_token(), future, fn, args, kwargs)
        )
        return future

    def stop(self) -> None:
        self._end.set()

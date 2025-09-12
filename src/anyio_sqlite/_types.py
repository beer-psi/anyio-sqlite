from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, Protocol, Union

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer

IsolationLevel = Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
SqliteData = Union[str, "ReadableBuffer", int, float, None]


class AggregateProtocol(Protocol):
    def step(self, value: int, /) -> object: ...
    def finalize(self) -> int: ...


class WindowAggregateClass(Protocol):
    step: Callable[..., object]
    inverse: Callable[..., object]

    def value(self) -> SqliteData: ...
    def finalize(self) -> SqliteData: ...


class StopRunning:
    def __init__(self, token: object):
        self.token = token

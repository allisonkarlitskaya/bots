import asyncio
import weakref
from collections.abc import Callable

_watchers = weakref.WeakKeyDictionary[object, set[Callable[[], None]]]()


def notify(*objs: object) -> None:
    for obj in objs:
        if watchers := _watchers.pop(obj, None):
            for watcher in watchers:
                watcher()


async def watch(*objs: object) -> None:
    future: asyncio.Future[None] = asyncio.Future()

    def done() -> None:
        future.set_result(None)

    sets = [_watchers.setdefault(obj, set()) for obj in objs]
    for s in sets:
        s.add(done)
    try:
        await future
    finally:
        for s in sets:
            s.remove(done)

# Copyright (C) 2024 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import collections
import logging
from typing import Hashable, Mapping, Sequence, TypeVar

logger = logging.getLogger(__name__)

# When mypy gets PEP 695 support: https://github.com/python/mypy/issues/15238
# type JsonValue = None | bool | int | float | Sequence[JsonValue] | Mapping[str, JsonValue]
JsonValue = None | bool | int | float | Sequence['JsonValue'] | Mapping[str, 'JsonValue']
JsonObject = Mapping[str, JsonValue]
K = TypeVar('K', bound=Hashable)
T = TypeVar('T')
V = TypeVar('V')


class AsyncQueue(collections.deque[T]):
    def __init__(self) -> None:
        self._nonempty = asyncio.Event()
        self._eof = False

    async def next(self) -> T | None:
        await self._nonempty.wait()
        return self[0] if self else None

    def done(self, item: T) -> None:
        assert self.popleft() is item
        if not self and not self._eof:
            self._nonempty.clear()

    def put(self, item: T) -> None:
        self.append(item)
        self._nonempty.set()

    def eof(self) -> None:
        self._nonempty.set()
        self._eof = True


# Simple LRU cache: when full, evict the least-recently `add()`-ed item.
class LRUCache(dict[K, V]):
    def __init__(self, max_items: int = 128) -> None:
        self.max_items = max_items

    def add(self, key: K, value: V) -> None:
        # In order to make sure the value gets inserted at the end, we need to
        # remove a previous value, otherwise it will just take its place.
        self.pop(key, None)
        self[key] = value
        while len(self) > self.max_items:
            oldest = next(iter(self))
            logger.debug('evicting cached data for %r', oldest)
            self.pop(oldest)

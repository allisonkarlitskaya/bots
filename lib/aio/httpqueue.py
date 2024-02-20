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
import hashlib
import json
import logging
from types import TracebackType
from typing import Mapping, NamedTuple, Self

import aiohttp

from .s3 import S3Key, s3_sign
from .util import AsyncQueue, JsonValue

logger = logging.getLogger(__name__)


class HttpRequest(NamedTuple):
    method: str
    url: str
    headers: Mapping[str, str]
    data: bytes = b''
    s3_key: S3Key | None = None


class HttpQueue:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self._queue = AsyncQueue[HttpRequest]()
        self._task: asyncio.Task[None] | None = None
        self._level: int = logging.DEBUG

    async def run_queue(self) -> None:
        while request := await self._queue.next():
            headers = request.headers

            if request.s3_key is not None:
                checksum = hashlib.sha256(request.data).hexdigest()
                headers = s3_sign(request.url, request.method, headers, checksum, request.s3_key)

            logger.log(self._level, '%s %s', request.method, request.url)
            async with self.session.request(request.method, request.url, data=request.data, headers=headers) as resp:
                logger.debug('response %r', resp)
            await asyncio.sleep(1.0)
            self._queue.done(request)

    def request(self, request: HttpRequest) -> None:
        assert self._queue is not None
        self._queue.put(request)

    async def __aenter__(self) -> Self:
        self._task = asyncio.create_task(self.run_queue())
        return self

    async def __aexit__(self,
                        exc_type: type[BaseException] | None,
                        exc_value: BaseException | None,
                        traceback: TracebackType | None) -> None:
        assert self._task

        if items := len(self._queue):
            logger.info('Waiting for %r queued HTTP requests to complete...', items)
            self._level = logging.INFO  # make the rest of the output a bit louder

        self._queue.eof()
        await self._task

    def post(self, url: str, body: JsonValue, headers: Mapping[str, str]) -> None:
        headers = {**headers, 'Content-Type': 'application/json'}
        self.request(HttpRequest('POST', url, headers, json.dumps(body).encode()))

    def s3_put(self, url: str, body: bytes, headers: Mapping[str, str], s3_key: S3Key) -> None:
        self.request(HttpRequest('PUT', url, headers, body, s3_key=s3_key))

    def s3_delete(self, url: str, s3_key: S3Key) -> None:
        self.request(HttpRequest('DELETE', url, {}, s3_key=s3_key))

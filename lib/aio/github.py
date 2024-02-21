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

import logging
from typing import Callable, Generic, Mapping, NamedTuple

import aiohttp

from .s3streamer import Status
from .util import JsonValue, LRUCache, T

logger = logging.getLogger(__name__)


class CacheEntry(NamedTuple, Generic[T]):
    conditions: Mapping[str, str]
    value: T


class GitHub:
    cache: LRUCache[tuple[str, Callable[[JsonValue], object]], CacheEntry]

    def __init__(self, session: aiohttp.ClientSession, url: str, token: str, user_agent: str) -> None:
        self.headers = {
            "User-Agent": user_agent,
            "Authorization": f'token {token}'
        }
        self.url = url
        self.session = session
        self.cache = LRUCache()

    def qualify(self, resource: str) -> str:
        return self.url + resource

    async def post(self, resource: str, body: JsonValue = None) -> None:
        logger.debug('posting to %r', resource)
        async with self.session.post(self.qualify(resource), json=body, headers=self.headers) as response:
            logger.debug('response %r', response)

    async def get(self, resource: str, reducer: Callable[[object], T]) -> T | None:
        cache_key = (resource, reducer)

        headers = dict(self.headers)
        cache_entry = self.cache.get(cache_key)
        if cache_entry is not None:
            headers.update(cache_entry.conditions)

        logger.debug('get %r %r %r', resource, reducer, cache_entry)
        async with self.session.get(self.qualify(resource), headers=headers) as response:
            condition_map = {'etag': 'if-none-match', 'last-modified': 'if-modified-since'}
            conditions = {c: response.headers[h] for h, c in condition_map.items() if h in response.headers}

            if cache_entry is not None and response.status == 304:
                self.cache.add(cache_key, cache_entry)
                logger.debug('  cache hit -- returning cached value')
                return cache_entry.value

            elif (response.status // 100) == 2:  # 2xx status
                value = reducer(await response.json())
                logger.debug('  cache miss -- caching and returning %r %r', conditions, value)
                self.cache.add(cache_key, CacheEntry(conditions, value))
                return value

            else:
                return None


class GitHubStatus(Status):
    def __init__(self, api: GitHub, repo: str, revision: str, context: str, link: str) -> None:
        logger.debug('GitHub repo %s context %s link %s', repo, context, link)
        self.api = api
        self.resource = f'repos/{repo}/statuses/{revision}'
        self.link = link
        self.context = context

    async def post(self, state: str, description: str) -> None:
        logger.debug('POST statuses/%s %s %s', self.resource, state, description)
        await self.api.post(self.resource, {
            'context': self.context, 'state': state, 'description': description, 'target_url': self.link
        })

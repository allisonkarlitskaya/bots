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
import codecs
import json
import locale
import logging
import mimetypes
import os
import textwrap
from typing import ClassVar, Collection

from ..constants import LIB_DIR
from .httpqueue import HttpQueue
from .s3 import S3Key

logger = logging.getLogger(__name__)


class Destination:
    location: str

    def has(self, filename: str) -> bool:
        raise NotImplementedError

    def write(self, filename: str, data: bytes) -> None:
        raise NotImplementedError

    def delete(self, filenames: Collection[str]) -> None:
        raise NotImplementedError


class LocalDestination(Destination):
    def __init__(self, location: str) -> None:
        self.location = location
        os.makedirs(self.location)

    def path(self, filename: str) -> str:
        return os.path.join(self.location, filename)

    def has(self, filename: str) -> bool:
        return os.path.exists(self.path(filename))

    def write(self, filename: str, data: bytes) -> None:
        print(f'Write {self.path(filename)}')
        with open(self.path(filename), 'wb+') as file:
            file.write(data)

    def delete(self, filenames: Collection[str]) -> None:
        for filename in filenames:
            print(f'Delete {self.path(filename)}')
            os.unlink(self.path(filename))


class S3Destination(Destination):
    def __init__(self, queue: HttpQueue, url: str, key: S3Key, acl: str = 'public-read') -> None:
        self.queue = queue
        self.location = url
        self.headers = {'x-amz-acl': acl}
        self.key = key

    def url(self, filename: str) -> str:
        return self.location + filename

    def has(self, filename: str) -> bool:
        raise NotImplementedError('use Index')

    def write(self, filename: str, data: bytes) -> None:
        content_type, content_encoding = mimetypes.guess_type(filename)
        headers = {**self.headers, 'Content-Type': content_type or 'text/plain'}
        if content_encoding:
            headers['Content-Encoding'] = content_encoding

        self.queue.s3_put(self.url(filename), data, headers, self.key)

    def delete(self, filenames: Collection[str]) -> None:
        # to do: multi-object delete API
        for filename in filenames:
            self.queue.s3_delete(self.url(filename), self.key)


class Status:
    def post(self, state: str, description: str) -> None:
        raise NotImplementedError


class LocalStatus(Status):
    def __init__(self, location: str) -> None:
        print(f'Writing logs to {location}')

    def post(self, state: str, description: str) -> None:
        print(f'Status [{state}] {description}')


class Index(Destination):
    files: set[str]

    def __init__(self, destination: Destination, filename: str = 'index.html') -> None:
        self.destination = destination
        self.filename = filename
        self.files = set()
        self.dirty = True

    def has(self, filename: str) -> bool:
        return filename in self.files

    def write(self, filename: str, data: bytes) -> None:
        self.destination.write(filename, data)
        self.files.add(filename)
        self.dirty = True

    def delete(self, filenames: Collection[str]) -> None:
        raise NotImplementedError

    def sync(self) -> None:
        if self.dirty:
            self.destination.write(self.filename, textwrap.dedent('''
                <html>
                  <body>
                    <h1>Directory listing for /</h1>
                    <hr>
                    <ul>''' + ''.join(f'''
                      <li><a href={f}>{f}</a></li> ''' for f in sorted(self.files)) + '''
                    </ul>
                  </body>
                </html>
                ''').encode('utf-8'))
            self.dirty = False


class AttachmentsDirectory:
    def __init__(self, destination: Destination, local_directory: str) -> None:
        self.destination = destination
        self.path = local_directory

    def scan(self) -> None:
        for subdir, _dirs, files in os.walk(self.path):
            for filename in files:
                path = os.path.join(subdir, filename)
                name = os.path.relpath(path, start=self.path)

                if not self.destination.has(name):
                    logger.debug('Uploading attachment %s', name)
                    with open(path, 'rb') as file:
                        data = file.read()
                    self.destination.write(name, data)


class ChunkedUploader:
    SIZE_LIMIT: ClassVar[int] = 1000000  # 1MB
    TIME_LIMIT: ClassVar[int] = 30       # 30s

    chunks: list[list[bytes]]
    suffixes: set[str]
    send_at: float | None

    def __init__(self, index: Index, filename: str) -> None:
        assert locale.getpreferredencoding() == 'UTF-8'
        self.input_decoder = codecs.getincrementaldecoder('UTF-8')(errors='replace')
        self.suffixes = {'chunks'}
        self.chunks = []
        self.index = index
        self.destination = index.destination
        self.filename = filename
        self.pending = b''
        self.timer: asyncio.TimerHandle | None = None

    def clear_timer(self) -> None:
        if self.timer:
            self.timer.cancel()
        self.timer = None

    def send_pending(self) -> None:
        # Consume the pending buffer into the chunks list.
        self.chunks.append([self.pending])
        self.pending = b''
        self.clear_timer()

        # 2048 algorithm.
        #
        # This can be changed to merge more or less often, or to never merge at
        # all. The only restriction is that it may only ever update the last
        # item in the list.
        while len(self.chunks) > 1 and len(self.chunks[-1]) == len(self.chunks[-2]):
            last = self.chunks.pop()
            second_last = self.chunks.pop()
            self.chunks.append(second_last + last)

        # Now we figure out how to send that last item.
        # Let's keep the client dumb: it doesn't need to know about blocks: only bytes.
        chunk_sizes = [sum(len(block) for block in chunk) for chunk in self.chunks]

        if chunk_sizes:
            last_chunk_start = sum(chunk_sizes[:-1])
            last_chunk_end = last_chunk_start + chunk_sizes[-1]
            last_chunk_suffix = f'{last_chunk_start}-{last_chunk_end}'
            self.destination.write(f'{self.filename}.{last_chunk_suffix}', b''.join(self.chunks[-1]))
            self.suffixes.add(last_chunk_suffix)

        self.destination.write(f'{self.filename}.chunks', json.dumps(chunk_sizes).encode('ascii'))

    def start(self, data: str) -> None:
        # Send the initial data immediately, to get the chunks file written out.
        self.pending = data.encode()
        self.send_pending()
        AttachmentsDirectory(self.index, f'{LIB_DIR}/s3-html').scan()

    def write(self, data: bytes, final: bool = False) -> None:
        # Transcode the data (if necessary), and ensure that it's complete characters
        self.pending += self.input_decoder.decode(data, final=final).encode('utf-8')

        if final:
            # We're about to delete all of the chunks, so don't bother with
            # anything still pending...
            self.clear_timer()

            everything = b''.join(b''.join(block for block in chunk) for chunk in self.chunks) + self.pending
            self.index.write(self.filename, everything)

            # If the client ever sees a 404, it knows that the streaming is over.
            self.destination.delete([f'{self.filename}.{suffix}' for suffix in self.suffixes])

        elif self.pending:
            if len(self.pending) > ChunkedUploader.SIZE_LIMIT:
                self.send_pending()

            elif self.timer is None:
                self.timer = asyncio.get_running_loop().call_later(ChunkedUploader.TIME_LIMIT, self.send_pending)

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

import hashlib
import hmac
import logging
import os
import sys
import time
import urllib.parse
from typing import Mapping, NamedTuple

from ..directories import xdg_config_home

logger = logging.getLogger(__name__)


class S3Key(NamedTuple):
    access: str
    secret: str


def get_key(hostname: str) -> S3Key | None:
    s3_key_dir = xdg_config_home('cockpit-dev/s3-keys', envvar='COCKPIT_S3_KEY_DIR')

    # ie: 'cockpit-images.eu.linode.com' then 'eu.linode.com', then 'linode.com'
    while '.' in hostname:
        try:
            with open(os.path.join(s3_key_dir, hostname)) as fp:
                access, secret = fp.read().split()
                return S3Key(access, secret)
        except ValueError:
            print(f'ignoring invalid content of {s3_key_dir}/{hostname}', file=sys.stderr)
        except FileNotFoundError:
            pass
        _, _, hostname = hostname.partition('.')  # strip a leading component

    return None


def is_key_present(url: str) -> bool:
    """Checks if an S3 key is available for the given url"""
    parsed = urllib.parse.urlparse(url)
    assert parsed.hostname is not None
    return get_key(parsed.hostname) is not None


def s3_sign(
    url: str, method: str, headers: Mapping[str, str], checksum: str, keys: S3Key
) -> Mapping[str, str]:
    """Signs an AWS request using the AWS4-HMAC-SHA256 algorithm

    Returns a dictionary of extra headers which need to be sent along with the request.
    If the method is PUT then the checksum of the data to be uploaded must be provided.
    @headers, if given, are a dict of additional headers to be signed (eg: `x-amz-acl`)
    """
    parsed = urllib.parse.urlparse(url)
    assert parsed.hostname is not None

    amzdate = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())

    # Header canonicalisation demands all header names in lowercase
    headers = {key.lower(): value for key, value in headers.items()}
    headers.update({'host': parsed.hostname, 'x-amz-content-sha256': checksum, 'x-amz-date': amzdate})
    headers_str = ''.join(f'{k}:{v}\n' for k, v in sorted(headers.items()))
    headers_list = ';'.join(sorted(headers))

    credential_scope = f'{amzdate[:8]}/any/s3/aws4_request'
    signing_key = f'AWS4{keys.secret}'.encode('ascii')
    for item in credential_scope.split('/'):
        signing_key = hmac.new(signing_key, item.encode('ascii'), hashlib.sha256).digest()

    algorithm = 'AWS4-HMAC-SHA256'
    canonical_request = f'{method}\n{parsed.path}\n{parsed.query}\n{headers_str}\n{headers_list}\n{checksum}'
    request_hash = hashlib.sha256(canonical_request.encode('ascii')).hexdigest()
    string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n{request_hash}'
    signature = hmac.new(signing_key, string_to_sign.encode('ascii'), hashlib.sha256).hexdigest()
    headers['Authorization'] = f'{algorithm} Credential={keys.access}/{credential_scope},' \
        f'SignedHeaders={headers_list},Signature={signature}'

    return headers

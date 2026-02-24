# Copyright (C) 2026 Red Hat, Inc.
# SPDX-License-Identifier: GPL-3.0-or-later

"""Testing Farm API client for remote job execution."""

import asyncio
import contextlib
import json
import os
from pathlib import Path
from typing import Self

import httpx

from lib.aio.git import get_git_upstream
from lib.aio.jobcontext import JobContext
from lib.aio.jsonutil import JsonObject, get_dict, get_str, typechecked
from lib.directories import xdg_config_home
from lib.jobqueue import JobSpecification

# Testing Farm API endpoint
TF_API_URL = 'https://api.dev.testing-farm.io/v0.1'


class TestingFarmClient(contextlib.AsyncExitStack):
    def __init__(self, *, api_url: str = TF_API_URL, api_key: str | None = None) -> None:
        super().__init__()
        if api_key is None:
            api_key = (
                os.environ.get('TESTING_FARM_API_TOKEN')
                or Path(xdg_config_home("cockpit-dev/testing-farm-token")).read_text().strip()
            )
        self.api_url = api_url
        self.api_key = api_key

    async def __aenter__(self) -> Self:
        self.http = await self.enter_async_context(httpx.AsyncClient())
        return self

    def get_request_url(self, request_id: str) -> str:
        return f'{self.api_url}/requests/{request_id}'

    async def get_request(self, request_id: str) -> JsonObject | None:
        """Fetch request status from Testing Farm API. Returns None on 404."""
        response = await self.http.get(self.get_request_url(request_id))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return typechecked(response.json(), dict)

    async def wait_for_artifacts(self, request_id: str, timeout: float = 30) -> str | None:
        """Poll until run.artifacts is available.

        Returns the artifacts URL, or None if timeout reached or job failed.
        See https://issues.redhat.com/browse/TFT-4379
        """
        async def poll() -> str | None:
            delay = 0.5
            while True:
                req = await self.get_request(request_id)
                if req is not None:
                    if run := get_dict(req, 'run', None):
                        if artifacts := get_str(run, 'artifacts', None):
                            return artifacts
                    if get_str(req, 'state', None) not in ('new', 'queued', 'running'):
                        return None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

        try:
            return await asyncio.wait_for(poll(), timeout=timeout)
        except TimeoutError:
            return None

    async def submit_job(
        self,
        ctx: JobContext,
        job: JobSpecification | JsonObject,  # TODO: PEP 728
        *,
        git_url_ref: tuple[str, str] | None = None,
        compose: str = 'Fedora-Rawhide',
    ) -> str:
        """Submit a job to Testing Farm for remote execution.

        Args:
            ctx: JobContext with configuration (will be serialized)
            job: Job specification as JSON object
            git_url_ref: Git repository URL and ref (default: from @{upstream})
            compose: Fedora compose to use (default: Fedora-Rawhide)

        Returns:
            Testing Farm request ID
        """
        if git_url_ref is None:
            git_url_ref = await get_git_upstream()

        git_url, git_ref = git_url_ref
        config_json = json.dumps(ctx.serialize())
        job_json = json.dumps(job)

        # https://api.dev.testing-farm.io/docs#operation/request_a_new_test_v0_1_requests_post
        request = {
            'test': {
                'fmf': {
                    'url': git_url,
                    'ref': git_ref,
                    'name': '/job-runner',
                }
            },
            'environments': [
                {
                    'arch': 'x86_64',
                    'os': {'compose': compose},
                    'variables': {
                        'JOB_JSON': job_json,
                    },
                    'secrets': {
                        'JOB_RUNNER_CONFIG_JSON': config_json,
                    },
                    'hardware': {
                        'virtualization': {
                            'is-virtualized': True,
                            'is-supported': True,
                        }
                    },
                }
            ],
            'settings': {
                'pipeline': {
                    'timeout': 120,
                }
            },
        }

        response = await self.http.post(
            f'{TF_API_URL}/requests',
            json=request,
            headers={'Authorization': f'Bearer {self.api_key}'},
        )
        response.raise_for_status()
        return get_str(typechecked(response.json(), dict), 'id')

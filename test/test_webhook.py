"""End-to-end tests for the FastAPI webhook server."""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import respx
import uvicorn

from lib.aio.webhook import app


def mock_github_api(request: httpx.Request) -> httpx.Response:
    """Return mock responses for GitHub/Forgejo API calls."""
    path = request.url.path

    # /repos/{owner}/{repo}/branches
    if '/branches' in path:
        return httpx.Response(200, json=[])

    # /repos/{owner}/{repo}/pulls
    if '/pulls' in path:
        return httpx.Response(200, json=[])

    # Default: empty response
    return httpx.Response(200, json={})


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Start server as async task and create client."""
    # Mock all external API calls during server lifecycle
    with respx.mock:
        # Pass through requests to localhost (our test server)
        respx.route(host__regex=r"^127\.0\.0\.1").pass_through()
        # Mock GitHub API
        respx.route(host="api.github.com").mock(side_effect=mock_github_api)
        # Mock Forgejo/Codeberg API
        respx.route(host="codeberg.org").mock(side_effect=mock_github_api)

        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        server = uvicorn.Server(config)

        task = asyncio.create_task(server.serve())

        # Wait for server to start
        while not server.started:
            await asyncio.sleep(0.01)

        port = server.servers[0].sockets[0].getsockname()[1]

        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
            yield client

        server.should_exit = True
        await task


async def test_server_starts(client: httpx.AsyncClient) -> None:
    """Server starts and /api/ returns valid JSON."""
    response = await client.get("/api/")
    assert response.status_code == 200
    data = response.json()
    assert "forges" in data


async def test_ci_js_compiles(client: httpx.AsyncClient) -> None:
    """TypeScript compiles on-demand."""
    response = await client.get("/ci.js")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/javascript"
    assert "LitElement" in response.text


async def test_static_index(client: httpx.AsyncClient) -> None:
    """Static index.html is served."""
    response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "forge-viewer" in response.text

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import re
import sys
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from lib.aio.brain import Brain, Job, Worker
from lib.aio.events import UnknownEvent, parse_repository_event
from lib.aio.jobcontext import JobContext
from lib.aio.jsonutil import JsonError, JsonObject, get_nested, get_str
from lib.aio.store import EntityStore

logger = logging.getLogger(__name__)
WEB_DIR = Path(__file__).parent / "web"


async def load_testmap() -> dict[str, dict[str, Mapping[str, Sequence[str]]]]:
    """Load REPO_BRANCH_CONTEXT via subprocess and transform to nested forge/repo structure."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'lib.testmap',
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    repo_branch_context = json.loads(stdout)

    testmap: dict[str, dict[str, Mapping[str, Sequence[str]]]] = defaultdict(dict)
    for repo, branch_context in repo_branch_context.items():
        forge, _, repo = repo.rpartition(':')
        if not forge and not repo.startswith('cockpit-project/'):
            continue
        testmap[forge or "github"][repo] = branch_context

    return testmap




async def sync_testmap_periodically(brain: Brain) -> None:
    """Reload testmap from disk every minute."""
    while True:
        await asyncio.sleep(60)
        try:
            testmap = await load_testmap()
            brain.sync_testmap(testmap)
        except Exception:
            logger.exception("Failed to reload testmap")


def load_secrets(ctx: JobContext) -> tuple[dict[str, bytes], str]:
    """Load secrets from config at startup. Returns (webhook_secrets, registration_secret)."""
    webhook_secrets: dict[str, bytes] = {}

    with get_nested(ctx.config, 'forge') as forges:
        for name in forges:
            if name == 'default':
                continue
            with get_nested(forges, name) as forge_config:
                secret = get_str(forge_config, 'webhook-secret', None)
                if secret:
                    if not secret.strip():
                        raise ValueError(f"forge.{name}.webhook-secret is empty")
                    webhook_secrets[name] = secret.encode()

    with get_nested(ctx.config, 'webhook') as webhook:
        with get_nested(webhook, 'workers') as workers:
            registration_secret = get_str(workers, 'registration-secret')
            if not registration_secret.strip():
                raise ValueError("webhook.workers.registration-secret is empty")

    return webhook_secrets, registration_secret


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    ctx = JobContext(None)
    async with ctx:
        webhook_secrets, registration_secret = load_secrets(ctx)

        brain = Brain(ctx)

        brain.sync_testmap(await load_testmap())
        tasks = [
            asyncio.create_task(brain.coldplug()),
            asyncio.create_task(sync_testmap_periodically(brain)),
        ]

        app.state.webhook_secrets = webhook_secrets
        app.state.registration_secret = registration_secret
        app.state.brain = brain
        app.state.store = EntityStore(brain)
        try:
            yield
        finally:
            app.state.store.shutdown()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(lifespan=lifespan)
api = FastAPI()
app.mount("/api", api)


# === Web UI ===


@app.get("/")
async def serve_index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/favicon.ico")
async def serve_favicon() -> FileResponse:
    return FileResponse(WEB_DIR / "favicon.ico")


@app.get("/ci.js")
async def serve_ci_js() -> Response:
    try:
        content = (WEB_DIR / "ci.js").read_text()
    except FileNotFoundError:
        proc = await asyncio.create_subprocess_exec(
            "esbuild", WEB_DIR / "ci.ts",
            stdout=asyncio.subprocess.PIPE,
        )
        content, _ = await proc.communicate()
    return Response(content, media_type="application/javascript")


@app.get("/health")
async def health() -> JsonObject:
    return {"status": "ok"}


# === Dependencies ===


def find_brain(req: Request) -> Brain:
    # Use root app's state, not the mounted api app
    brain = app.state.brain
    assert isinstance(brain, Brain)
    return brain


def find_store(req: Request) -> EntityStore:
    # Use root app's state, not the mounted api app
    store = app.state.store
    assert isinstance(store, EntityStore)
    return store


# === Generic GET handler ===


@api.get("/{path:path}")
async def get_entity(
    request: Request,
    path: str,
    store: Annotated[EntityStore, Depends(find_store)],
    if_none_match: Annotated[str | None, Header()] = None,
    prefer: Annotated[str | None, Header()] = None,
) -> Response:
    # Parse timeout from Prefer header
    timeout = 0.0
    if prefer and (match := re.search(r"\bwait\s*=\s*(\d+)", prefer)):
        timeout = float(match.group(1))

    # Normalize path
    path = path.strip("/") if path else ""

    def make_headers(**extra: str) -> dict[str, str]:
        return {"Server-Monotonic-Now": str(time.monotonic()), **extra}

    try:
        if timeout > 0 and if_none_match:
            # Long-poll: wait for change
            entry = await store.watch_path(path, if_none_match, timeout)
            if entry is None:
                return Response(status_code=304, headers=make_headers(ETag=if_none_match))
            return Response(
                content=entry.json_str,
                media_type="application/json",
                headers=make_headers(ETag=entry.etag, **{"Preference-Applied": f"wait={int(timeout)}"}),
            )
        else:
            # Immediate response
            entry = store.get_cached(path)

            if entry.etag == if_none_match:
                return Response(status_code=304, headers=make_headers(ETag=entry.etag))

            return Response(
                content=entry.json_str,
                media_type="application/json",
                headers=make_headers(ETag=entry.etag),
            )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


# === Queue endpoints ===


def find_brain_checked(brain: Annotated[Brain, Depends(find_brain)]) -> Brain:
    brain.queue_check()
    return brain


registration_secret = HTTPBearer(scheme_name="registration-secret")
worker_cookie = HTTPBearer(scheme_name="worker-cookie")


@api.post("/workers")
async def post_worker(
    brain: Annotated[Brain, Depends(find_brain_checked)],
    auth: Annotated[HTTPAuthorizationCredentials, Depends(registration_secret)],
    name: Annotated[str, Body()],
    capacity: Annotated[int, Body()],
    topics: Annotated[list[str], Body()],
) -> JSONResponse:
    if not hmac.compare_digest(auth.credentials, app.state.registration_secret):
        raise HTTPException(401, "Invalid registration secret")
    worker = brain.add_worker(name=name, capacity=capacity, topics=topics)
    return JSONResponse({"id": worker.id, "cookie": worker._cookie})


def find_worker_rw(
    brain: Annotated[Brain, Depends(find_brain_checked)],
    nr: int,
    auth: Annotated[HTTPAuthorizationCredentials, Depends(worker_cookie)],
) -> Worker:
    try:
        worker = brain.get_worker(nr)
    except KeyError as exc:
        raise HTTPException(404, "Worker not found") from exc

    if not hmac.compare_digest(auth.credentials, worker._cookie):
        raise HTTPException(401, "Cookie is incorrect")

    # We just got a request with the worker's cookie, so we know that we're
    # currently hearing from the worker.
    brain.worker_touch(worker)
    return worker


@api.patch("/workers/{nr}")
async def patch_worker(
    worker: Annotated[Worker, Depends(find_worker_rw)],
    request: Request,
) -> None:
    if await request.json() != {"capacity": 0}:
        raise HTTPException(400, "Invalid request")
    worker.brain.worker_soft_shutdown(worker)


@api.post("/workers/{nr}/heartbeat")
async def post_worker_heartbeat(
    worker: Annotated[Worker, Depends(find_worker_rw)],
) -> None:
    worker.brain.worker_touch(worker)


@api.delete("/workers/{nr}")
async def delete_worker(
    worker: Annotated[Worker, Depends(find_worker_rw)],
) -> None:
    worker.brain.worker_kill(worker)


def find_job_rw(
    brain: Annotated[Brain, Depends(find_brain_checked)],
    job_id: int,
    auth: Annotated[HTTPAuthorizationCredentials, Depends(worker_cookie)],
) -> Job:
    try:
        job = brain.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(404, "Job not found") from exc

    if job.worker is None:
        raise HTTPException(400, "Job is not assigned to any worker")

    if not hmac.compare_digest(auth.credentials, job.worker._cookie):
        raise HTTPException(401, "Cookie does not match job's worker")

    # We just got a request with the worker's cookie, so we know that we're
    # currently hearing from the worker.
    brain.worker_touch(job.worker)
    return job


@api.delete("/jobs/{job_id}")
async def delete_job(
    job: Annotated[Job, Depends(find_job_rw)],
) -> None:
    job.brain.finish_job(job)


# === Webhook ===


def webhook_event(brain: Brain, forge_name: str, event_name: str, body: JsonObject) -> str:
    try:
        forge = brain.forges[forge_name]
    except KeyError:
        return f"Forge {forge_name!r} doesn't appear in testmap"

    match event_name:
        case 'ping':
            return "pong"

        case 'pull_request' | 'pull' | 'push' | 'delete' | 'status':
            with get_nested(body, "repository") as repository_json:
                repo_name = get_str(repository_json, "full_name")

            try:
                repository = forge.repos[repo_name]
            except KeyError:
                return f"Repository {repo_name!r} doesn't appear in testmap"

            logger.warning("  - %s", repo_name)

            try:
                event = parse_repository_event(event_name, body)
            except UnknownEvent as exc:
                return str(exc)

            logger.warning("  - %r", event)
            return repository.received_webhook_event(event)

        case _:
            return f"Don't know how to handle {event_name!r} event"


@api.post("/webhook/{forge}")
async def post_webhook(
    brain: Annotated[Brain, Depends(find_brain)],
    request: Request,
    *,
    forge: str,
    event: str = Header(alias='X-GitHub-Event'),
    signature: str = Header(alias='X-Hub-Signature-256'),
) -> str:
    try:
        secret = app.state.webhook_secrets[forge]
    except KeyError as exc:
        raise HTTPException(404, detail="Invalid forge name or no webhook-secret configured") from exc

    raw = await request.body()
    expected = hmac.new(secret, raw, hashlib.sha256).hexdigest()
    if signature != f"sha256={expected}":
        raise HTTPException(401, detail="Invalid signature")

    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(422, detail="Invalid JSON. Check webhook content type.") from exc

    try:
        logger.warning('webhook/%s: %r (%r bytes):', forge, event, len(raw))
        result = webhook_event(brain, forge, event, body)
        logger.warning('    → %r', result)
        return result
    except JsonError as exc:
        logger.error('    → failed: %r', exc)
        raise HTTPException(422, detail=str(exc)) from exc

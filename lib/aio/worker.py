import argparse
import asyncio
import logging
import random
import signal
from collections.abc import AsyncIterable, Iterable

import httpx

from lib.aio.jsonutil import JsonObject

logger = logging.getLogger(__name__)


async def register_worker(
    client: httpx.AsyncClient,
    name: str,
    topics: Iterable[str],
    capacity: int,
    shared_secret: str,
) -> tuple[int, str]:
    resp = await client.post(
        "/api/workers",
        json={"name": name, "topics": list(topics), "capacity": capacity},
        headers={"Authorization": f"Bearer {shared_secret}"},
    )
    resp.raise_for_status()
    data = resp.json()
    return int(data["id"]), str(data["cookie"])


async def heartbeat_loop(
    client: httpx.AsyncClient,
    worker_id: int,
    cookie: str,
    interval_seconds: float = 30.0,
) -> None:
    try:
        while True:
            try:
                resp = await client.post(
                    f"/api/workers/{worker_id}/heartbeat",
                    content=b"",
                    headers={"Authorization": f"Bearer {cookie}"},
                    timeout=10.0,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.warning("[heartbeat] error: %r", e)
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        return


async def fetch_job_description(client: httpx.AsyncClient, job_id: int) -> JsonObject:
    resp = await client.get(f"/api/jobs/{job_id}", timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def do_work(job_id: int, job: JsonObject) -> None:
    logger.info("[job %d] starting: %s", job_id, job)
    await asyncio.sleep(5.0)
    logger.info("[job %d] done", job_id)


async def job_runner(client: httpx.AsyncClient, job_id: int, cookie: str) -> None:
    try:
        job = await fetch_job_description(client, job_id)
        await do_work(job_id, job)
        resp = await client.delete(
            f"/api/jobs/{job_id}",
            headers={"Authorization": f"Bearer {cookie}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        logger.info("[job %d] deleted remotely", job_id)
    except asyncio.CancelledError:
        logger.info("[job %d] cancelled", job_id)
        raise
    except Exception as e:
        logger.error("[job %d] error: %r", job_id, e)


async def long_poll_workers(client: httpx.AsyncClient, worker_id: int) -> AsyncIterable[list[int]]:
    etag: str | None = None
    while True:
        headers = {
            # After the first fetch we use If-None-Match and Prefer: wait= to
            # implement a push notification mechanism.  This combination
            # signals the server to delay its reply if there was no change.
            "If-None-Match": etag,
            # In case all the workers started at once, let them drift apart over
            # time so that they don't all hit the server together at the top of
            # every minute.  Keep it below 60s, though to avoid network/proxy
            # timeouts.
            "Prefer": f"wait={random.randrange(40, 59)}"
        } if etag is not None else {}

        try:
            resp = await client.get(f"/api/workers/{worker_id}", headers=headers, timeout=120)
            if resp.status_code == 304:
                continue
            resp.raise_for_status()
            etag = resp.headers.get("ETag")
            data = resp.json()
            jobs = [int(j) for j in data.get("jobs", [])]
            yield jobs
        except Exception as e:
            logger.warning("[poll] error: %r; backing off", e)
            await asyncio.sleep(2.0)


async def supervisor(client: httpx.AsyncClient, worker_id: int, cookie: str) -> None:
    running: dict[int, asyncio.Task[None]] = {}
    try:
        async for jobs in long_poll_workers(client, worker_id):
            want: set[int] = set(jobs)
            have: set[int] = set(running.keys())

            for job_id in sorted(want - have):
                logger.info("[supervisor] starting job %d", job_id)
                t = asyncio.create_task(job_runner(client, job_id, cookie), name=f"job-{job_id}")
                running[job_id] = t

            for job_id in sorted(have - want):
                logger.info("[supervisor] cancelling job %d", job_id)
                task = running.pop(job_id, None)
                if task and not task.done():
                    task.cancel()

            done_ids = [jid for jid, t in running.items() if t.done()]
            for jid in done_ids:
                try:
                    await running[jid]
                except Exception:
                    pass
                running.pop(jid, None)
    except asyncio.CancelledError:
        for t in running.values():
            if not t.done():
                t.cancel()
        await asyncio.gather(*running.values(), return_exceptions=True)
        return


async def main() -> None:
    parser = argparse.ArgumentParser(description="cockpit-ci worker client")
    parser.add_argument("--endpoint", required=True, help="Base URL")
    parser.add_argument("--name", required=True, help="Worker name")
    parser.add_argument("--capacity", type=int, required=True, help="Worker capacity")
    parser.add_argument(
        "--topic",
        dest="topics",
        action="append",
        default=[],
        help="Topic (repeatable).",
    )
    parser.add_argument(
        "--shared-secret",
        default="hihi",
        help='Shared secret for /api/workers registration (default: "hihi")',
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (per-request).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    async with httpx.AsyncClient(
        base_url=args.endpoint,
        http2=True,
        timeout=10.0,
    ) as client:
        worker_id, cookie = await register_worker(
            client,
            name=args.name,
            topics=args.topics,
            capacity=args.capacity,
            shared_secret=args.shared_secret,
        )
        logger.info("[worker] id=%d registered", worker_id)

        hb = asyncio.create_task(heartbeat_loop(client, worker_id, cookie), name="heartbeat")
        sup = asyncio.create_task(supervisor(client, worker_id, cookie), name="supervisor")

        loop = asyncio.get_running_loop()
        stop = asyncio.Event()

        def _signal_handler() -> None:
            stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass

        await stop.wait()
        logger.info("[main] shutting down...")
        for t in (hb, sup):
            t.cancel()
        await asyncio.gather(hb, sup, return_exceptions=True)

        await client.delete(
            f"/api/workers/{worker_id}",
            headers={"Authorization": f"Bearer {cookie}"},
        )


if __name__ == "__main__":
    asyncio.run(main())

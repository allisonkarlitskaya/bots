import bisect
import logging
import secrets
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass, field
from typing import Annotated

from lib.aio.entity import Entity, IntKey, Parent, StrKey
from lib.aio.events import (
    DeleteEvent,
    PullCloseEvent,
    PullLabelEvent,
    PullSyncEvent,
    PushEvent,
    RepositoryEvent,
    Sha,
    StatusEvent,
)
from lib.aio.github import GitHub
from lib.aio.jobcontext import JobContext
from lib.aio.jsonutil import JsonObject, get_int, get_nested, get_objv, get_str, typechecked

logger = logging.getLogger(__name__)


# === Value types (not entities, no parent/key) ===


@dataclass(eq=False, slots=True)
class Status:
    state: str
    description: str | None
    target_url: str | None

    def json(self) -> JsonObject:
        return {
            "state": self.state,
            "description": self.description,
            "target_url": self.target_url,
        }


@dataclass(frozen=True, slots=True)
class Error:
    worker: str
    log: str | None

    def json(self) -> JsonObject:
        return {"worker": self.worker, "log": self.log}


# === Entities ===


@dataclass(eq=False, slots=True, weakref_slot=True)
class Job(Entity):
    brain: Annotated[Brain, Parent(via="jobs")]
    id: IntKey
    topic: str
    priority: int
    birth: float
    entry: float
    errors: list[Error] = field(default_factory=list)
    payload: Mapping[str, object] | None = None
    worker: Worker | None = None
    context: Context | None = None

    def json(self) -> JsonObject:
        return {"id": self.id, "topic": self.topic}


@dataclass(eq=False, slots=True, weakref_slot=True)
class Worker(Entity):
    brain: Annotated[Brain, Parent(via="workers")]
    id: IntKey
    name: str
    topics: tuple[str, ...]
    capacity: int
    liveness: float = field(default_factory=time.monotonic)
    jobs: Set[Job] = field(default_factory=frozenset)
    _cookie: str = field(default_factory=lambda: secrets.token_urlsafe(32))


@dataclass(eq=False, slots=True, weakref_slot=True)
class Context(Entity):
    commit: Annotated[Commit, Parent(via="contexts")]
    name: StrKey
    status: Status
    job: Job | None = None


@dataclass(eq=False, slots=True, weakref_slot=True)
class Commit(Entity):
    repo: Annotated[Repository, Parent(via="commits")]
    sha: StrKey
    why: Set[Pull | Branch] = field(default_factory=frozenset)
    contexts: Mapping[str, Context] | None = None


@dataclass(eq=False, slots=True)
class Pull(Entity):
    repo: Annotated[Repository, Parent(via="pulls")]
    number: IntKey
    head: Commit
    target_ref: str
    title: str
    labels: Set[str] = field(default_factory=frozenset)


@dataclass(eq=False, slots=True)
class Branch(Entity):
    repo: Annotated[Repository, Parent(via="branches")]
    name: StrKey
    head: Commit


@dataclass(eq=False, slots=True, weakref_slot=True)
class Repository(Entity):
    forge: Annotated[Forge, Parent(via="repos")]
    name: StrKey
    commits: Mapping[Sha, Commit] = field(default_factory=dict)
    pulls: Mapping[int, Pull] = field(default_factory=dict)
    branches: Mapping[str, Branch] = field(default_factory=dict)
    _webhook_queue: list[RepositoryEvent] | None = field(default_factory=list)
    testmap: Mapping[str, Sequence[str]] = field(default_factory=dict)

    def _get_commit(self, sha: Sha) -> Commit:
        """Get or create a commit. Caller must immediately add a reason."""
        if sha in self.commits:
            return self.commits[sha]

        commit = Commit(repo=self, sha=sha)
        self.commits = {**self.commits, sha: commit}
        return commit

    def _commit_add_reason(self, commit: Commit, reason: Branch | Pull) -> None:
        commit.why = {*commit.why, reason}

    def ensure_contexts(self, commit: Commit) -> None:
        """Create contexts from testmap if commit has none.

        Finds Pull reasons for this commit and populates contexts from testmap.
        Skips if commit already has contexts (e.g., from fetched GitHub statuses).
        """
        from lib.aio.policy import context_job_params, should_create_contexts

        if commit.contexts is not None and len(commit.contexts) > 0:
            return  # already has contexts

        for reason in commit.why:
            if not isinstance(reason, Pull):
                continue

            if (target_ref := should_create_contexts(reason)) is None:
                continue
            if target_ref not in self.testmap:
                continue

            if commit.contexts is None:
                commit.contexts = {}

            for context_name in self.testmap[target_ref]:
                if context_name in commit.contexts:
                    continue
                params = context_job_params(
                    forge=self.forge.name,
                    repo=self.name,
                    sha=commit.sha,
                    context=context_name,
                    pull=reason,
                )
                if params is None:
                    continue
                job = self.forge.brain.add_job(
                    topic=get_str(params, "topic"),
                    payload=params,
                    priority=get_int(params, "priority"),
                )
                if job.worker is not None:
                    description = f"Queued on {job.worker.name}"
                else:
                    description = "Waiting for worker"
                status = Status(state="pending", description=description, target_url=None)
                context = Context(commit=commit, name=context_name, status=status, job=job)
                job.context = context
                commit.contexts = {**commit.contexts, context_name: context}

    def _commit_drop_reason(self, commit: Commit, reason: Branch | Pull) -> None:
        commit.why = commit.why - {reason}
        if commit.why:
            return  # still tracked for another reason

        # Stop watching this commit
        self.commits = {k: v for k, v in self.commits.items() if k != commit.sha}

        # Cancel any running jobs
        if commit.contexts is not None:
            for context in commit.contexts.values():
                if context.job is not None:
                    self.forge.brain.finish_job(context.job)

    def pull_sync(
        self,
        number: int,
        head_sha: Sha,
        target_ref: str,
        title: str,
        labels: Sequence[str],
        *,
        ensure_contexts: bool = True,
    ) -> str:
        logger.debug("pull_sync(%r, %r, %r)", self.name, head_sha, target_ref)

        if number in self.pulls:
            pull = self.pulls[number]
            if pull.head.sha == head_sha and pull.target_ref == target_ref:
                return "no change"
            self._commit_drop_reason(pull.head, pull)

        # Create new pull (even if updating, we recreate)
        commit = self._get_commit(head_sha)
        pull = Pull(
            repo=self,
            number=number,
            head=commit,
            target_ref=target_ref,
            title=title,
            labels=frozenset(labels),
        )
        self._commit_add_reason(commit, pull)
        self.pulls = {**self.pulls, number: pull}

        # Mark commit for CI tracking if testmap has entry for this target_ref
        if commit.contexts is None and target_ref in self.testmap:
            commit.contexts = {}

        if ensure_contexts:
            self.ensure_contexts(commit)

        return "synced"

    def pull_close(self, number: int) -> str:
        logger.debug("pull_close(%r, %r)", self.name, number)

        if number not in self.pulls:
            return "unknown"

        pull = self.pulls[number]
        self._commit_drop_reason(pull.head, pull)
        self.pulls = {k: v for k, v in self.pulls.items() if k != number}
        return "closed"

    def push(self, ref: str, head_sha: str) -> str:
        logger.debug("push(%r, %r, %r)", self.name, ref, head_sha)

        if ref in self.branches:
            branch = self.branches[ref]
            if branch.head.sha == head_sha:
                return "no change"
            self._commit_drop_reason(branch.head, branch)

        # Create new branch
        commit = self._get_commit(head_sha)
        branch = Branch(repo=self, name=ref, head=commit)
        self._commit_add_reason(commit, branch)
        self.branches = {**self.branches, ref: branch}
        return "updated"

    def delete(self, ref: str) -> str:
        if ref not in self.branches:
            return "unknown"

        branch = self.branches[ref]
        self._commit_drop_reason(branch.head, branch)
        self.branches = {k: v for k, v in self.branches.items() if k != ref}
        return "deleted"

    def status(self, sha: Sha, context_name: str, state: str, description: str | None, target_url: str | None) -> str:
        if sha not in self.commits:
            return "unknown sha"

        commit = self.commits[sha]
        if commit.contexts is None:
            return "not tracking statuses for this commit"

        new_status = Status(state, description, target_url)

        if context_name in commit.contexts:
            commit.contexts[context_name].status = new_status
            return "updated state of existing context"
        else:
            context = Context(commit=commit, name=context_name, status=new_status)
            commit.contexts = {**commit.contexts, context_name: context}
            return "added new context"

    def process_event(self, event: RepositoryEvent) -> str:
        match event:
            case PullSyncEvent(number, head, target, title, labels):
                return self.pull_sync(number, head, target, title, labels)
            case PullLabelEvent():
                return 'todo'
            case PullCloseEvent(number):
                return self.pull_close(number)
            case PushEvent(branch, head):
                return self.push(branch, head)
            case DeleteEvent(branch):
                return self.delete(branch)
            case StatusEvent(sha, context, state, description, target_url):
                return self.status(sha, context, state, description, target_url)

    def received_webhook_event(self, event: RepositoryEvent) -> str:
        if self._webhook_queue is not None:
            self._webhook_queue.append(event)
            return "queued"
        else:
            return self.process_event(event)

    def sync_testmap(self, testmap: Mapping[str, Sequence[str]]) -> None:
        self.testmap = testmap
        # Reassignment triggers invalidation via __setattr__

    def teardown(self) -> None:
        for commit in self.commits.values():
            if commit.contexts is None:
                continue
            for context in commit.contexts.values():
                if context.job is not None:
                    self.forge.brain.finish_job(context.job)


@dataclass(eq=False, slots=True, weakref_slot=True)
class Forge(Entity):
    brain: Annotated[Brain, Parent(via="forges")]
    name: StrKey
    _driver: GitHub
    repos: Mapping[str, Repository] = field(default_factory=dict)
    limits: JsonObject = field(default_factory=dict)

    def sync_testmap(self, testmap: Mapping[str, Mapping[str, Sequence[str]]]) -> None:
        have = set(self.repos)
        wanted = set(testmap)

        new_repos = dict(self.repos)

        for repo_name in wanted - have:
            new_repos[repo_name] = Repository(self, repo_name)

        for repo_name in have - wanted:
            new_repos[repo_name].teardown()
            del new_repos[repo_name]

        self.repos = new_repos  # triggers invalidation

        for name, repo in self.repos.items():
            repo.sync_testmap(testmap[name])

    def teardown(self) -> None:
        for repo in self.repos.values():
            repo.teardown()


@dataclass(eq=False, slots=True, weakref_slot=True)
class Brain(Entity):
    _ctx: JobContext
    forges: Mapping[str, Forge] = field(default_factory=dict)
    workers: Mapping[int, Worker] = field(default_factory=dict)
    jobs: Mapping[int, Job] = field(default_factory=dict)

    # Queue internal state
    _last_id: int = 0
    _max_errors: int = 5
    _capacity: defaultdict[str, set[Worker]] = field(default_factory=lambda: defaultdict(set))
    _backlog: defaultdict[str, list[Job]] = field(default_factory=lambda: defaultdict(list))

    def _next_id(self) -> int:
        self._last_id += 1
        return self._last_id

    # === Queue dispatch methods ===

    def queue_check(self) -> None:
        """Assert queue invariants (expensive, for testing only)."""
        capacity: defaultdict[str, set[Worker]] = defaultdict(set)
        backlog: defaultdict[str, list[Job]] = defaultdict(list)

        for job_id, job in self.jobs.items():
            assert job.id == job_id
            if job.worker is not None:
                assert self.workers[job.worker.id] is job.worker
                assert job in job.worker.jobs
            else:
                backlog[job.topic].append(job)

        for worker_id, worker in self.workers.items():
            assert worker.id == worker_id
            if len(worker.jobs) < worker.capacity:
                for topic in worker.topics:
                    capacity[topic].add(worker)
            for job in worker.jobs:
                assert self.jobs[job.id] is job
                assert job.worker is worker

        topics = set(capacity) | set(backlog) | set(self._capacity) | set(self._backlog)
        for topic in topics:
            assert self._capacity[topic] == capacity[topic]
            assert self._backlog[topic] == sorted(
                backlog[topic], key=lambda j: (-j.priority, j.entry)
            )

    def _worker_needs_work(self, worker: Worker) -> None:
        queues = [self._backlog[topic] for topic in worker.topics]
        jobs_to_add: set[Job] = set()

        while len(worker.jobs) + len(jobs_to_add) < worker.capacity:
            candidates = [q[0] for q in queues if q]
            if not candidates:
                for topic in worker.topics:
                    self._capacity[topic].add(worker)
                break

            job = min(candidates, key=lambda j: (-j.priority, j.entry))
            self._backlog[job.topic].remove(job)
            jobs_to_add.add(job)
            self._assign_job_to_worker(job, worker)

        if jobs_to_add:
            worker.jobs = frozenset(worker.jobs | jobs_to_add)

    def _assign_job_to_worker(self, job: Job, worker: Worker) -> None:
        """Assign a job to a worker and update context status."""
        job.worker = worker
        if job.context is not None:
            job.context.status = Status(
                state="pending",
                description=f"Queued on {worker.name}",
                target_url=job.context.status.target_url,
            )

    def _queue_job(self, job: Job) -> None:
        assert job.brain is self
        assert self.jobs[job.id] is job
        assert job.worker is None

        if workers := self._capacity[job.topic]:
            worker = next(iter(workers))
            assert len(worker.jobs) < worker.capacity

            worker.jobs = frozenset(worker.jobs | {job})
            self._assign_job_to_worker(job, worker)

            if worker.capacity <= len(worker.jobs):
                for topic in worker.topics:
                    self._capacity[topic].discard(worker)
        else:
            bisect.insort(
                self._backlog[job.topic], job, key=lambda j: (-j.priority, j.entry)
            )

    def get_job(self, job_id: int) -> Job:
        return self.jobs[job_id]

    def add_job(self, topic: str, payload: Mapping[str, object], priority: int) -> Job:
        now = time.monotonic()
        job_id = self._next_id()
        job = Job(
            brain=self,
            id=job_id,
            topic=topic,
            priority=priority,
            birth=now,
            entry=now,
            payload=payload,
        )
        self.jobs = {**self.jobs, job_id: job}
        self._queue_job(job)
        return job

    def fail_job(self, job: Job, reason: str) -> None:
        assert job.brain is self
        worker = job.worker
        assert worker is not None

        worker.jobs = frozenset(worker.jobs - {job})
        self._worker_needs_work(worker)
        job.worker = None

        job.errors = [*job.errors, Error(worker.name, reason)]
        if len(job.errors) < self._max_errors:
            job.entry = time.monotonic()
            self._queue_job(job)
        else:
            self.jobs = {k: v for k, v in self.jobs.items() if k != job.id}

    def finish_job(self, job: Job) -> None:
        assert job.brain is self

        if worker := job.worker:
            worker.jobs = frozenset(worker.jobs - {job})
            self._worker_needs_work(worker)
        else:
            self._backlog[job.topic].remove(job)

        self.jobs = {k: v for k, v in self.jobs.items() if k != job.id}

    def get_worker(self, worker_id: int) -> Worker:
        return self.workers[worker_id]

    def add_worker(self, *, name: str, capacity: int, topics: Sequence[str]) -> Worker:
        worker_id = self._next_id()
        worker = Worker(
            brain=self,
            id=worker_id,
            name=name,
            capacity=capacity,
            topics=tuple(topics),
        )
        self.workers = {**self.workers, worker_id: worker}
        self._worker_needs_work(worker)
        return worker

    def worker_soft_shutdown(self, worker: Worker) -> None:
        assert worker.brain is self

        if len(worker.jobs) < worker.capacity:
            for topic in worker.topics:
                self._capacity[topic].discard(worker)

        worker.capacity = 0

    def worker_touch(self, worker: Worker) -> None:
        assert worker.brain is self
        worker.liveness = time.monotonic()

    def worker_kill(self, worker: Worker, reason: str = "Worker killed") -> None:
        assert worker.brain is self

        if len(worker.jobs) < worker.capacity:
            for topic in worker.topics:
                self._capacity[topic].discard(worker)

        worker.capacity = 0

        for job in set(worker.jobs):
            self.fail_job(job, reason)

        self.workers = {k: v for k, v in self.workers.items() if k != worker.id}

    def reap_workers(self, duration: float) -> None:
        cutoff = time.monotonic() - duration
        to_kill = [w for w in self.workers.values() if w.liveness < cutoff]

        for worker in to_kill:
            self.worker_soft_shutdown(worker)
        for worker in to_kill:
            self.worker_kill(worker, reason=f"No heartbeat in >{duration}s")

    # === Testmap sync ===

    def sync_testmap(self, testmap: Mapping[str, Mapping[str, Mapping[str, Sequence[str]]]]) -> None:
        logger.debug("Webhook: synchronising testmap")
        have = set(self.forges)
        wanted = set(testmap)

        new_forges = dict(self.forges)

        for name in wanted - have:
            logger.debug("Testmap added forge %r", name)
            try:
                driver = self._ctx._forges[name]
            except KeyError:
                logger.error("Ignoring forge %r in testmap, unknown to job-runner", name)
                continue

            assert isinstance(driver, GitHub)
            forge = Forge(self, name, driver)

            def notify_limits(info: JsonObject, f: Forge = forge) -> None:
                f.limits = info

            driver._limit_info_notify = notify_limits
            new_forges[name] = forge

        for name in have - wanted:
            logger.debug("Testmap removed forge %r", name)
            new_forges[name].teardown()
            del new_forges[name]

        self.forges = new_forges  # triggers invalidation

        for name, forge in self.forges.items():
            forge.sync_testmap(testmap[name])

    async def coldplug(self) -> None:
        logger.warning("Coldplugging the brain")
        for forge in self.forges.values():
            logger.warning("  - forge %r:", forge.name)
            for repo in forge.repos.values():
                if repo._webhook_queue is None:
                    logger.warning("    - repo %r: already coldplugged", repo.name)
                    return

                logger.warning("    - repo %r:", repo.name)

                # Fetch branches
                logger.warning("      - branches:")
                branches = typechecked(
                    await repo.forge._driver.get(f'repos/{repo.name}/branches'),
                    list,
                )
                logger.warning("        - [%r branches]", len(branches))
                for branch in branches:
                    name = get_str(branch, "name")
                    with get_nested(branch, "commit") as commit_json:
                        if 'sha' in commit_json:
                            sha = get_str(commit_json, "sha")  # GitHub
                        else:
                            sha = get_str(commit_json, "id")  # Forgejo
                    repo.push(name, sha)

                # Fetch pulls
                logger.warning("      - pulls:")
                pulls = typechecked(
                    await repo.forge._driver.get(f'repos/{repo.name}/pulls', {"state": "open"}),
                    list,
                )
                logger.warning("        - [%r pulls]", len(pulls))
                for pull in pulls:
                    number = get_int(pull, "number")
                    title = get_str(pull, "title")
                    labels = get_objv(pull, "labels", lambda v: get_str(v, "name"))
                    with get_nested(pull, "base") as base_json:
                        ref = get_str(base_json, "ref")
                    with get_nested(pull, "head") as head_json:
                        sha = get_str(head_json, "sha")

                    repo.pull_sync(number, sha, ref, title, labels, ensure_contexts=False)

                # Fetch statuses for commits that have Pull reasons
                logger.warning("      - %r commits:", len(repo.commits))
                for tracked_commit in repo.commits.values():
                    has_pull = any(isinstance(r, Pull) for r in tracked_commit.why)
                    if not has_pull:
                        logger.warning("        - %r: branch-only, skipping", tracked_commit.sha)
                        continue

                    # Initialize contexts so status() will accept incoming statuses
                    tracked_commit.contexts = {}

                    logger.warning("        - %r:", tracked_commit.sha)
                    statuses = await forge._driver.get_statuses(repo.name, tracked_commit.sha)
                    logger.warning("          - [%r statuses]", len(statuses))

                    for context_name, status_json in statuses.items():
                        state = get_str(status_json, "state", None)
                        if state is None:
                            continue
                        description = get_str(status_json, "description", None)
                        target_url = get_str(status_json, "target_url", None)
                        repo.status(tracked_commit.sha, context_name, state, description, target_url)

                    # Fill in from testmap if no statuses found on GitHub
                    repo.ensure_contexts(tracked_commit)

                # Process delayed webhook queue
                for event in repo._webhook_queue:
                    repo.process_event(event)
                repo._webhook_queue = None

        logger.warning("Coldplugging complete.")
        logger.warning(
            "Watching %r commits in %r repositories across %r forges.",
            sum(len(repo.commits) for forge in self.forges.values() for repo in forge.repos.values()),
            sum(len(forge.repos) for forge in self.forges.values()),
            len(self.forges),
        )

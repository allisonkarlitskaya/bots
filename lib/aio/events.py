from collections.abc import Sequence
from dataclasses import dataclass

from lib.aio.jsonutil import JsonObject, get_int, get_nested, get_objv, get_str

type Sha = str


@dataclass(eq=False, slots=True)
class PullSyncEvent:
    number: int
    head: Sha
    target: str
    title: str
    labels: Sequence[str]


@dataclass(eq=False, slots=True)
class PullLabelEvent:
    number: int
    action: str
    modified: str
    labels: Sequence[str]


@dataclass(eq=False, slots=True)
class PullCloseEvent:
    number: int


@dataclass(eq=False, slots=True)
class PushEvent:
    ref: str
    head: Sha


@dataclass(eq=False, slots=True)
class DeleteEvent:
    ref: str


@dataclass(eq=False, slots=True)
class StatusEvent:
    sha: Sha
    context: str
    state: str
    description: str | None
    target_url: str | None


type RepositoryEvent = PullSyncEvent | PullLabelEvent | PullCloseEvent | PushEvent | DeleteEvent | StatusEvent


class UnknownEvent(Exception):
    pass


def parse_repository_event(event: str, body: JsonObject) -> RepositoryEvent:
    match event:
        case 'pull_request':
            with get_nested(body, "pull_request") as pull_request:
                number = get_int(pull_request, "number")

                match get_str(body, "action"):
                    case 'opened' | 'synchronized' | 'synchronize':
                        title = get_str(pull_request, "title")
                        labels = get_objv(pull_request, "labels", lambda label: get_str(label, "name"))

                        # TODO: how does labels work?
                        with get_nested(pull_request, "head") as head:
                            head_sha = get_str(head, "sha")
                        with get_nested(pull_request, "base") as base:
                            base_ref = get_str(base, "ref")

                        return PullSyncEvent(number, head_sha, base_ref, title, labels)

                    case 'labeled' | 'unlabeled' as action:
                        labels = get_objv(pull_request, "labels", lambda label: get_str(label, "name"))
                        with get_nested(body, "label") as label:
                            modified = get_str(label, "name")
                        return PullLabelEvent(number, action, modified, labels)

                    case 'closed':
                        return PullCloseEvent(number)

                    case action:
                        raise UnknownEvent(f"Unknown pull request action {action!r}")

        case 'push':
            ref = get_str(body, "ref")
            if not ref.startswith("refs/heads/"):
                raise UnknownEvent(f"ref {ref!r} is not a head")
            branch = ref.removeprefix("refs/heads/")
            after = get_str(body, "after")

            # GitHub sends "0000000000000000000000000000000000000000" for delete
            if all(c == '0' for c in after):
                return DeleteEvent(branch)
            else:
                return PushEvent(ref, after)

        case 'delete':
            ref = get_str(body, "ref")
            if not ref.startswith("refs/heads/"):
                raise UnknownEvent(f"ref {ref!r} is not a head")
            branch = ref.removeprefix("refs/heads/")
            return DeleteEvent(branch)

        case 'status':
            sha = get_str(body, "sha")
            context = get_str(body, "context")
            state = get_str(body, "state")
            description = get_str(body, "description", None)
            target_url = get_str(body, "target_url", None)
            return StatusEvent(sha, context, state, description, target_url)

        case event:
            raise UnknownEvent(f"Unknown event {event!r}")

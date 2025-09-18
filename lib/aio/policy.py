"""Policy function for determining job parameters from context."""

from lib.aio.brain import Branch, Pull
from lib.aio.jsonutil import JsonObject


def should_create_contexts(reason: Branch | Pull) -> str | None:
    """Return target_ref if we should create contexts for this reason, None otherwise."""
    match reason:
        case Branch():
            return None
        case Pull(target_ref=target_ref, labels=labels, title=title):
            if "no-test" in labels:
                return None
            if "[no-test]" in title:
                return None
            return target_ref


def context_job_params(
    forge: str,
    repo: str,
    sha: str,
    context: str,
    pull: Pull,
    bots_ref: str = "main",
) -> JsonObject | None:
    """Return job parameters for a context, or None if no job should be created.

    The returned dict contains:
      - topic: queue name for the job
      - priority: job priority (higher = more urgent)
      - ... and all JobSpecification fields (repo, sha, context, slug, env, secrets, etc.)
    """
    from lib.testmap import get_default_branch, split_context

    target_ref = pull.target_ref

    topic = "rhel-test" if "rhel" in context else "test"
    priority = 1 if "/devel" in context else 0

    image_scenario, bots_pr, context_project, context_branch = split_context(context)
    image, _, scenario = image_scenario.partition("/")

    # Determine the command subject (what repo/branch to actually test)
    command_subject: JsonObject | None
    if context_project:
        # Context specifies a different repo to test (e.g., "fedora-42@cockpit-project/cockpit")
        context_forge, _, project = context_project.rpartition(":")
        branch = context_branch or get_default_branch(context_project)
        command_subject = {
            "forge": context_forge or None,
            "repo": project,
            "branch": branch,
        }
    else:
        command_subject = None

    # Build the slug
    slug_suffix = context.replace("/", "-").replace("@", "-")
    slug = f"pull-{pull.number}-{sha[:8]}-{slug_suffix}"

    # Build environment variables
    env: dict[str, str] = {
        "TEST_OS": image,
        "TEST_REVISION": sha,
        "TEST_PULL": str(pull.number),
    }
    if scenario:
        env["TEST_SCENARIO"] = scenario
    if bots_pr:
        # TODO: resolve bots PR to a sha
        env["COCKPIT_BOTS_REF"] = f"pull/{bots_pr}/head"
    elif bots_ref:
        env["COCKPIT_BOTS_REF"] = bots_ref
    if target_ref:
        env["BASE_BRANCH"] = target_ref

    # Secrets needed by the test
    secrets = ["github-token", "image-download"]
    if repo == "rhinstaller/anaconda-webui":
        secrets.extend(["fedora-wiki", "fedora-wiki-staging"])

    return {
        "topic": topic,
        "priority": priority,
        "forge": forge,
        "repo": repo,
        "sha": sha,
        "context": context,
        "pull": pull.number,
        "slug": slug,
        "env": env,
        "secrets": secrets,
        "command_subject": command_subject,
    }

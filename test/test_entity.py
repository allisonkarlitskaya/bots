import asyncio
from collections.abc import Mapping, Set
from dataclasses import dataclass, field
from typing import Annotated

import pytest

from lib.aio.entity import (
    Entity,
    IntKey,
    Parent,
    StrKey,
    watch,
)
from lib.aio.store import EntityStore

# === Test fixtures: simple entity hierarchy ===


@dataclass(eq=False, slots=True)
class Value:
    """Non-entity with json() method."""
    state: str
    description: str | None

    def json(self) -> dict[str, str | None]:
        return {"state": self.state, "description": self.description}


@dataclass(eq=False, slots=True, weakref_slot=True)
class Root(Entity):
    nodes: Mapping[int, Node] = field(default_factory=dict)
    _secret: str = "hidden"


@dataclass(eq=False, slots=True, weakref_slot=True)
class Node(Entity):
    root: Annotated[Root, Parent(via="nodes")]
    id: IntKey
    leaves: Mapping[str, Leaf] = field(default_factory=dict)
    refs: Set[Leaf] = field(default_factory=frozenset)


@dataclass(eq=False, slots=True, weakref_slot=True)
class Leaf(Entity):
    parent: Annotated[Node, Parent(via="leaves")]
    name: StrKey
    value: Value


# === Tests for Entity basics ===


def test_entity_parent() -> None:
    """Entity._parent() returns the parent entity."""
    root = Root()
    node = Node(root=root, id=42)

    assert root._parent() is None
    assert node._parent() is root


def test_entity_key() -> None:
    """Entity._key() returns the key field value."""
    root = Root()
    node = Node(root=root, id=42)
    leaf = Leaf(parent=node, name="test", value=Value("ok", None))

    assert root._key() is None
    assert node._key() == 42
    assert leaf._key() == "test"


# === Tests for Entity notification ===


@pytest.mark.asyncio
async def test_entity_notify_on_setattr() -> None:
    """Setting an attribute notifies watchers."""
    root = Root()
    notified = []

    async def wait_and_record() -> None:
        await watch(root, "nodes")
        notified.append("nodes")

    task = asyncio.create_task(wait_and_record())
    await asyncio.sleep(0.01)  # Let the watcher register

    root.nodes = {1: Node(root=root, id=1)}
    await asyncio.sleep(0.01)  # Let the notification propagate

    assert notified == ["nodes"]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_entity_watch_any_property() -> None:
    """Watching with prop=None catches any property change."""
    root = Root()
    notified = []

    async def wait_any() -> None:
        await watch(root, None)
        notified.append("any")

    task = asyncio.create_task(wait_any())
    await asyncio.sleep(0.01)

    root.nodes = {}  # Trigger change
    await asyncio.sleep(0.01)

    assert notified == ["any"]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_entity_notify_propagates_up_parent_chain() -> None:
    """Changing a descendant notifies watchers on ancestor collections."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    leaf = Leaf(parent=node, name="a", value=Value("ok", None))
    node.leaves = {"a": leaf}

    notified = []

    async def wait_on_root_nodes() -> None:
        await watch(root, "nodes")
        notified.append("root.nodes")

    async def wait_on_node_leaves() -> None:
        await watch(node, "leaves")
        notified.append("node.leaves")

    task1 = asyncio.create_task(wait_on_root_nodes())
    task2 = asyncio.create_task(wait_on_node_leaves())
    await asyncio.sleep(0.01)

    # Change the leaf - should propagate up
    leaf.value = Value("changed", "new description")
    await asyncio.sleep(0.01)

    # Both watchers should be notified
    assert "node.leaves" in notified
    assert "root.nodes" in notified

    for task in [task1, task2]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_entity_notify_propagates_with_correct_field_names() -> None:
    """Propagation uses the correct field name at each level."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}

    # Watch for "nodes" specifically (not "leaves" or anything else)
    notified = []

    async def wait_on_nodes() -> None:
        await watch(root, "nodes")
        notified.append("nodes")

    task = asyncio.create_task(wait_on_nodes())
    await asyncio.sleep(0.01)

    # Change node.leaves - should propagate to root.nodes
    node.leaves = {"x": Leaf(parent=node, name="x", value=Value("ok", None))}
    await asyncio.sleep(0.01)

    assert notified == ["nodes"]

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# === Tests for EntityStore path computation ===


def test_store_path_of_root() -> None:
    """Root entity has empty path."""
    root = Root()
    store = EntityStore(root)
    assert store.path_of(root) == ""


def test_store_path_of_nested() -> None:
    """Nested entities have correct paths."""
    root = Root()
    node = Node(root=root, id=42)
    root.nodes = {42: node}
    leaf = Leaf(parent=node, name="x", value=Value("ok", None))
    node.leaves = {"x": leaf}

    store = EntityStore(root)
    assert store.path_of(node) == "/nodes/42"
    assert store.path_of(leaf) == "/nodes/42/leaves/x"


# === Tests for EntityStore navigation ===


def test_store_navigate_to_root() -> None:
    """Empty path navigates to root."""
    root = Root()
    store = EntityStore(root)

    entity, prop = store.navigate("")
    assert entity is root
    assert prop is None


def test_store_navigate_to_entity() -> None:
    """Navigate to nested entity."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    store = EntityStore(root)

    entity, prop = store.navigate("nodes/1")
    assert entity is node
    assert prop is None


def test_store_navigate_to_property() -> None:
    """Navigate to entity property."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    store = EntityStore(root)

    entity, prop = store.navigate("nodes/1/leaves")
    assert entity is node
    assert prop == "leaves"


def test_store_navigate_not_found() -> None:
    """Navigation raises KeyError for missing paths."""
    root = Root()
    store = EntityStore(root)

    with pytest.raises(KeyError):
        store.navigate("nodes/999")


# === Tests for EntityStore JSON serialization ===


def test_store_json_basic() -> None:
    """Basic JSON serialization of root entity."""
    root = Root()
    store = EntityStore(root)
    j = store.json_at("")

    assert j == {"nodes": {}}
    assert "_secret" not in j  # underscore-prefixed fields excluded


def test_store_json_with_children() -> None:
    """JSON serialization expands owned children."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    store = EntityStore(root)

    j = store.json_at("")
    assert j == {"nodes": {"1": {"id": 1, "leaves": {}, "refs": []}}}


def test_store_json_nested() -> None:
    """JSON serialization of deeply nested entities."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    leaf = Leaf(parent=node, name="a", value=Value("pending", "waiting"))
    node.leaves = {"a": leaf}
    store = EntityStore(root)

    j = store.json_at("")
    assert j == {
        "nodes": {
            "1": {
                "id": 1,
                "leaves": {
                    "a": {
                        "name": "a",
                        "value": {"state": "pending", "description": "waiting"},
                    }
                },
                "refs": [],
            }
        }
    }


def test_store_json_reference_not_expanded() -> None:
    """JSON serialization shows only key for non-owned references."""
    root = Root()
    node1 = Node(root=root, id=1)
    node2 = Node(root=root, id=2)
    root.nodes = {1: node1, 2: node2}

    # leaf is owned by node1
    leaf = Leaf(parent=node1, name="a", value=Value("ok", None))
    node1.leaves = {"a": leaf}

    # node2 references a leaf it doesn't own
    node2.refs = frozenset({leaf})

    store = EntityStore(root)
    j = store.json_at("")

    # The leaf in node1's 'leaves' is expanded (node1 owns it)
    assert j["nodes"]["1"]["leaves"]["a"]["name"] == "a"
    # But in node2's 'refs', it's just the key (node2 doesn't own it)
    assert j["nodes"]["2"]["refs"] == ["a"]


def test_store_json_property() -> None:
    """JSON serialization of a specific property."""
    root = Root()
    node = Node(root=root, id=1)
    root.nodes = {1: node}
    leaf = Leaf(parent=node, name="a", value=Value("ok", None))
    node.leaves = {"a": leaf}
    store = EntityStore(root)

    j = store.json_at("nodes/1/leaves")
    assert j == {"a": {"name": "a", "value": {"state": "ok", "description": None}}}


# === Tests for EntityStore caching ===


def test_store_cache_basic() -> None:
    """EntityStore caches JSON responses."""
    root = Root()
    store = EntityStore(root)

    entry1 = store.get_cached("")
    entry2 = store.get_cached("")

    assert entry1 is entry2
    assert entry1.etag


def test_store_cache_etag_changes() -> None:
    """Cache etag changes when content changes."""
    root = Root()
    store = EntityStore(root)

    entry1 = store.get_cached("")
    etag1 = entry1.etag

    # Modify and invalidate
    root.nodes = {1: Node(root=root, id=1)}
    store._invalidate("")

    entry2 = store.get_cached("")
    assert entry2.etag != etag1


@pytest.mark.asyncio
async def test_store_watch_path_immediate_change() -> None:
    """watch_path returns immediately if etag differs."""
    root = Root()
    store = EntityStore(root)
    entry = store.get_cached("")

    result = await store.watch_path("", "different-etag", 1.0)
    assert result is not None
    assert result.etag == entry.etag


@pytest.mark.asyncio
async def test_store_watch_path_timeout() -> None:
    """watch_path returns None on timeout."""
    root = Root()
    store = EntityStore(root)
    entry = store.get_cached("")

    result = await store.watch_path("", entry.etag, 0.1)
    assert result is None

"""Minimal Entity base class with property-level change notification.

This module provides the core entity infrastructure without any knowledge of
JSON serialization or caching. Those concerns belong to the mapping layer.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, get_origin, get_type_hints

# === Markers for Annotated[] ===


@dataclass(frozen=True, slots=True, kw_only=True)
class Parent:
    """Marker: this field references the parent entity.

    Args:
        via: The name of the property on the parent that contains this entity.
             Required for path generation and targeted notification.
    """
    via: str


class Key:
    """Marker: this field is the entity's key within its parent."""


# === Type aliases ===


StrKey = Annotated[str, Key()]
IntKey = Annotated[int, Key()]


# === Property-level notification ===

_watcher_dicts: dict[int, dict[str | None, set[Callable[[], None]]]] = {}
_global_notify_callbacks: list[Callable[["Entity", str], None]] = []


def add_notify_callback(callback: Callable[["Entity", str], None]) -> None:
    """Register a callback to be called on every entity property change."""
    _global_notify_callbacks.append(callback)


def _get_watcher_dict(entity: "Entity") -> dict[str | None, set[Callable[[], None]]]:
    """Get or create the watcher dict for an entity."""
    entity_id = id(entity)
    if entity_id not in _watcher_dicts:
        _watcher_dicts[entity_id] = {}
    return _watcher_dicts[entity_id]


def _cleanup_watcher_dict(entity_id: int) -> None:
    """Remove empty watcher dict."""
    if entity_id in _watcher_dicts:
        d = _watcher_dicts[entity_id]
        if not any(d.values()):
            del _watcher_dicts[entity_id]


def _notify_one(entity: "Entity", prop: str | None) -> None:
    """Notify watchers on a single (entity, prop) without propagation."""
    entity_id = id(entity)
    watcher_dict = _watcher_dicts.get(entity_id)
    if watcher_dict is None:
        return

    to_call: list[Callable[[], None]] = []

    # Wake watchers for specific property
    if prop is not None and prop in watcher_dict:
        to_call.extend(watcher_dict[prop])

    # Wake watchers for any property
    if None in watcher_dict:
        to_call.extend(watcher_dict[None])

    for callback in to_call:
        callback()


def _notify(entity: "Entity", prop: str) -> None:
    """Notify that an entity property changed, propagating up the parent chain.

    Wakes:
    - Watchers on (entity, prop)
    - Watchers on (entity, None)
    - Watchers on each ancestor for the collection containing the descendant
    """
    # Call global callbacks
    for callback in _global_notify_callbacks:
        callback(entity, prop)

    # Notify this entity
    _notify_one(entity, prop)

    # Propagate up the parent chain
    current: Entity = entity
    while True:
        collection = current._in_parent_field()
        parent = current._parent()
        if parent is None or collection is None:
            break
        _notify_one(parent, collection)
        current = parent


async def watch(entity: "Entity", prop: str | None = None) -> None:
    """Wait until the entity (or specific property) changes."""
    future: asyncio.Future[None] = asyncio.Future()

    def done() -> None:
        if not future.done():
            future.set_result(None)

    entity_id = id(entity)
    watcher_dict = _get_watcher_dict(entity)
    watchers = watcher_dict.setdefault(prop, set())
    watchers.add(done)

    try:
        await future
    finally:
        watchers.discard(done)
        if not watchers:
            del watcher_dict[prop]
        _cleanup_watcher_dict(entity_id)


# === Entity base class ===


class Entity:
    """Base class for entities with parent/key tracking and change notification.

    This is intentionally minimal. Path computation, JSON serialization, and
    caching are handled by the mapping layer (EntityStore).

    Entity relationship invariant (when all three fields are set):

        self.<parent_field>.<in_parent_field>[self.<key_field>] == self

    Example: Context has parent_field="commit", in_parent_field="contexts",
    key_field="name", so:

        context.commit.contexts[context.name] is context
    """

    # Lazily resolved by _ensure_metadata()
    _entity_parent_field: ClassVar[str | None]      # field on self pointing to parent
    _entity_in_parent_field: ClassVar[str | None]   # field on parent containing self
    _entity_key_field: ClassVar[str | None]         # field on self used as key
    _entity_fields: ClassVar[dict[str, Any]]

    @classmethod
    def _ensure_metadata(cls) -> None:
        """Lazily resolve type hints and extract entity metadata."""
        if '_entity_fields' in cls.__dict__:
            return  # Already resolved for this class

        hints = get_type_hints(cls, include_extras=True)

        parent_field: str | None = None
        in_parent_field: str | None = None
        key_field: str | None = None
        entity_fields: dict[str, Any] = {}

        for name, hint in hints.items():
            if name.startswith('_') or get_origin(hint) is ClassVar:
                continue

            entity_fields[name] = hint
            markers = hint.__metadata__ if get_origin(hint) is Annotated else ()

            for marker in markers:
                if isinstance(marker, Parent):
                    if parent_field is not None:
                        raise TypeError(f"{cls.__name__} has multiple Parent fields")
                    parent_field = name
                    in_parent_field = marker.via
                elif isinstance(marker, Key):
                    if key_field is not None:
                        raise TypeError(f"{cls.__name__} has multiple Key fields")
                    key_field = name

        if parent_field is not None and in_parent_field is None:
            raise TypeError(f"{cls.__name__}: Parent() requires a collection name")

        cls._entity_parent_field = parent_field
        cls._entity_in_parent_field = in_parent_field
        cls._entity_key_field = key_field
        cls._entity_fields = entity_fields

    def _parent(self) -> "Entity | None":
        """Return this entity's parent, or None if it's a root."""
        type(self)._ensure_metadata()
        if self._entity_parent_field is None:
            return None
        return getattr(self, self._entity_parent_field, None)

    def _key(self) -> Any:
        """Return this entity's key value."""
        type(self)._ensure_metadata()
        if self._entity_key_field:
            return getattr(self, self._entity_key_field)
        return None

    def _in_parent_field(self) -> str | None:
        """Return the field name on parent that contains this entity."""
        type(self)._ensure_metadata()
        return self._entity_in_parent_field

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)

        if name.startswith('_'):
            return

        _notify(self, name)

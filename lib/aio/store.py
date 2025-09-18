"""REST mapping layer for entities.

EntityStore provides:
- Path computation from entity relationships
- JSON serialization with ownership-based expansion
- Navigation from paths to entities/properties
- Caching with ETag-based long-polling
- Translation of entity notifications to cache invalidations
"""

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, get_args, get_origin

from .entity import Entity, Parent, add_notify_callback, watch


@dataclass(slots=True)
class CacheEntry:
    json_str: str
    etag: str


class EntityStore:
    """REST adapter for an entity tree.

    Handles path-based access, JSON serialization, and caching with
    long-polling support. Each store instance has its own cache.
    """

    def __init__(self, root: Entity) -> None:
        self.root = root
        self._cache: dict[str, CacheEntry] = {}
        self._waiters: dict[str, list[asyncio.Future[None]]] = {}

        # Invalidate cache on any entity change
        add_notify_callback(self._on_entity_changed)

    def shutdown(self) -> None:
        """Cancel all pending long-poll waiters."""
        for futures in self._waiters.values():
            for future in futures:
                if not future.done():
                    future.cancel()
        self._waiters.clear()

    def _on_entity_changed(self, entity: Entity, prop: str) -> None:
        """Called when any entity property changes."""
        # Check if entity is in our tree by walking up to root
        # May fail during entity construction when not all fields are set
        try:
            current: Entity | None = entity
            while current is not None:
                if current is self.root:
                    # Entity is in our tree - invalidate
                    path = self.path_of(entity)
                    self._invalidate(path)
                    return
                current = current._parent()
        except AttributeError:
            # Entity still being constructed
            pass

    # === Path computation ===

    def path_of(self, entity: Entity) -> str:
        """Compute the path to an entity from the root."""
        entity.__class__._ensure_metadata()

        if entity is self.root:
            return ""

        parent = entity._parent()
        if parent is None:
            raise ValueError(f"{entity} has no parent")

        parent_path = self.path_of(parent)
        collection = entity._entity_in_parent_field
        key = entity._key()

        return f"{parent_path}/{collection}/{key}"

    # === Navigation ===

    def navigate(self, path: str) -> tuple[Entity, str | None]:
        """Navigate to an entity and optional property by path.

        Returns (entity, property_name) where property_name is None if
        the path points to the entity itself.
        """
        if not path or path == "/":
            return (self.root, None)

        segments = path.strip("/").split("/")
        current: Entity = self.root
        i = 0

        while i < len(segments):
            seg = segments[i]
            current.__class__._ensure_metadata()

            # Check if this segment is a collection field on the current entity
            field_hint = current._entity_fields.get(seg)
            if field_hint is None:
                raise KeyError(f"Unknown field {seg!r} on {current.__class__.__name__}")

            field_value = getattr(current, seg)

            # If this is the last segment, return the property
            if i + 1 >= len(segments):
                return (current, seg)

            # Otherwise, we need to descend into a collection
            origin = get_origin(field_hint)
            if origin is None or not issubclass(origin, Mapping):
                raise KeyError(f"Cannot descend into non-mapping field {seg!r}")

            # Get the key from the next segment(s)
            # Handle special case: some keys contain slashes (e.g., "owner/repo")
            args = get_args(field_hint)
            key_type = args[0] if args else str

            key_str = segments[i + 1]

            # Try to convert key to the appropriate type
            key: Any = key_str
            if key_type is int:
                try:
                    key = int(key_str)
                except ValueError:
                    pass

            # If key not found, try combining segments (for "owner/repo" style keys)
            if key not in field_value and i + 2 < len(segments):
                combined_key = f"{segments[i + 1]}/{segments[i + 2]}"
                if combined_key in field_value:
                    key = combined_key
                    i += 1  # Skip extra segment

            if key not in field_value:
                raise KeyError(f"{key!r} not found in {seg}")

            next_value = field_value[key]
            if not isinstance(next_value, Entity):
                raise KeyError("Cannot descend into non-entity value")

            current = next_value
            i += 2

        return (current, None)

    def get(self, path: str) -> Any:
        """Get the value at a path.

        Returns:
        - For entity paths: the entity
        - For property paths: the property value
        - For collection paths without key: list of keys
        """
        entity, prop = self.navigate(path)

        if prop is None:
            return entity

        return getattr(entity, prop)

    # === JSON serialization ===

    def to_json(self, value: Any, owner: Entity | None = None) -> Any:
        """Serialize a value to JSON.

        Args:
            value: The value to serialize
            owner: The entity that owns this value (for ownership checks)
        """
        if value is None:
            return None

        # Entity: expand if owned, otherwise just the key
        if isinstance(value, Entity):
            if owner is not None and self._owns(owner, value):
                return self._entity_to_json(value)
            else:
                return value._key()

        # Non-Entity with json() method
        if hasattr(value, 'json') and callable(value.json):
            return value.json()

        # Mapping
        if isinstance(value, Mapping):
            result = {}
            for k, v in value.items():
                if isinstance(v, Entity):
                    if owner is not None and self._owns(owner, v):
                        result[str(k)] = self._entity_to_json(v)
                    else:
                        result[str(k)] = v._key()
                else:
                    result[str(k)] = self.to_json(v, owner)
            return result

        # Set/frozenset
        if isinstance(value, (set, frozenset)):
            items = []
            for v in value:
                if isinstance(v, Entity):
                    if owner is not None and self._owns(owner, v):
                        items.append(self._entity_to_json(v))
                    else:
                        items.append(v._key())
                else:
                    items.append(self.to_json(v, owner))
            return items

        # List/tuple/Sequence
        if isinstance(value, (list, tuple, Sequence)) and not isinstance(value, str):
            return [self.to_json(v, owner) for v in value]

        # Primitive
        return value

    def _owns(self, owner: Entity, other: Entity) -> bool:
        """Check if owner owns other (other's parent is owner)."""
        return other._parent() is owner

    def _entity_to_json(self, entity: Entity) -> dict[str, Any]:
        """Serialize an entity to JSON dict."""
        entity.__class__._ensure_metadata()
        result: dict[str, Any] = {}

        for name, hint in entity._entity_fields.items():
            # Skip parent fields (back-references)
            markers = hint.__metadata__ if get_origin(hint) is Annotated else ()
            if any(isinstance(m, Parent) for m in markers):
                continue

            value = getattr(entity, name, None)
            result[name] = self.to_json(value, entity)

        return result

    def json_at(self, path: str) -> Any:
        """Get the JSON representation of the value at a path."""
        entity, prop = self.navigate(path)

        if prop is None:
            return self._entity_to_json(entity)

        value = getattr(entity, prop)
        return self.to_json(value, entity)

    # === Caching ===

    def _cache_key(self, path: str) -> str:
        return path or "/"

    def _invalidate(self, path: str) -> None:
        """Invalidate cache entries affected by a path change."""
        # Invalidate this path and all ancestors/descendants
        to_invalidate = [
            p for p in list(self._cache.keys())
            if p.startswith(path) or path.startswith(p)
        ]
        for p in to_invalidate:
            del self._cache[p]

        # Wake waiters
        to_wake = [
            p for p in list(self._waiters.keys())
            if p.startswith(path) or path.startswith(p)
        ]
        for p in to_wake:
            for future in self._waiters.pop(p, []):
                if not future.done():
                    future.set_result(None)

    def get_cached(self, path: str) -> CacheEntry:
        """Get a cached JSON response for a path, computing if needed."""
        key = self._cache_key(path)
        if key not in self._cache:
            json_str = json.dumps(self.json_at(path))
            etag = hashlib.sha256(json_str.encode()).hexdigest()
            self._cache[key] = CacheEntry(json_str, etag)
        return self._cache[key]

    async def watch_path(
        self,
        path: str,
        current_etag: str | None,
        timeout: float,
    ) -> CacheEntry | None:
        """Watch a path until its content changes from current_etag.

        Returns the new CacheEntry, or None if timeout reached.
        """
        deadline = asyncio.get_event_loop().time() + timeout
        key = self._cache_key(path)

        # Find the entity and property we're watching
        try:
            entity, prop = self.navigate(path)
        except KeyError:
            return None

        while True:
            entry = self.get_cached(path)

            if entry.etag != current_etag:
                return entry

            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None

            # Wait for entity notification
            # We need to invalidate cache when entity changes
            watch_task = asyncio.create_task(watch(entity, prop))

            # Also track for path-based wakeup (in case of external invalidation)
            future: asyncio.Future[None] = asyncio.Future()
            self._waiters.setdefault(key, []).append(future)

            try:
                done, pending = await asyncio.wait(
                    [watch_task, future],
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                if not done:
                    # Timeout
                    return None

                # Invalidate cache since something changed
                self._invalidate(path)

            finally:
                # Clean up waiter
                if key in self._waiters:
                    waiters = self._waiters[key]
                    if future in waiters:
                        waiters.remove(future)
                    if not waiters:
                        del self._waiters[key]

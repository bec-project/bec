"""Store live queue entries and bounded history in departure order."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

_Item = TypeVar("_Item")


@dataclass
class _CacheEntry(Generic[_Item]):
    item: _Item | None = None
    is_result: bool = False


class ScanQueueCache(Generic[_Item]):
    """Keep live entries and bounded histories of results and other departures.

    Each history category retains at most maxlen entries, so cancelled backlogs cannot
    evict results and cancellation outcomes remain observable.

    The owning storage synchronizes access. Iteration and indexing expose history
    followed by live entries, with each object appearing once.
    """

    def __init__(self, maxlen: int, key: Callable[[_Item], str]) -> None:
        if maxlen is not None and maxlen < 0:
            raise ValueError("maxlen must be non-negative")
        self.maxlen = maxlen
        self._key = key
        self._live: dict[str, _CacheEntry[_Item]] = {}
        self._history: OrderedDict[str, _CacheEntry[_Item]] = OrderedDict()

    def retain(self, live_ids: Iterable[str], *, result_ids: Iterable[str] | None = None) -> None:
        """Refresh live IDs, remembering which entries have started producing results."""
        live = dict.fromkeys(live_ids)
        results = set(live if result_ids is None else result_ids)
        for key in live:
            entry = self._live.get(key) or self._history.pop(key, None) or _CacheEntry()
            entry.is_result |= key in results
            live[key] = entry
        for key, entry in self._live.items():
            if key not in live and entry.item is not None:
                self._history[key] = entry
                self._history.move_to_end(key)
        self._live = live
        self._trim_history()

    def _trim_history(self) -> None:
        if self.maxlen is None:
            return
        # Bound results and other departures independently. A cancelled request still
        # needs an observable outcome even when result history is full.
        counts = {True: 0, False: 0}
        for entry in self._history.values():
            counts[entry.is_result] += 1
        for key in list(self._history):
            is_result = self._history[key].is_result
            if counts[is_result] > self.maxlen:
                del self._history[key]
                counts[is_result] -= 1

    def append(self, item: _Item) -> None:
        """Store an item in the live queue or in bounded history until it is queued."""
        key = self._key(item)
        if key in self._live:
            self._live[key].item = item
        else:
            self._history[key] = _CacheEntry(item=item)
            self._history.move_to_end(key)
            self._trim_history()

    def is_live(self, key: str) -> bool:
        """Return whether the latest queue snapshot includes this ID."""
        return key in self._live

    def get(self, key: str) -> _Item | None:
        """Find an entry in the live queue or history."""
        entry = self._live.get(key) or self._history.get(key)
        return entry.item if entry is not None else None

    def __iter__(self) -> Iterator[_Item]:
        for entry in (*self._history.values(), *self._live.values()):
            if entry.item is not None:
                yield entry.item

    def __len__(self) -> int:
        return len(self._history) + sum(entry.item is not None for entry in self._live.values())

    def __getitem__(self, index: int) -> _Item:
        return tuple(self)[index]

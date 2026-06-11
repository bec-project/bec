from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Iterable, NamedTuple

import louie

from bec_lib.redis_connector.validation import error_log_with_context


@dataclass
class StreamSubInfo:
    cb_ref: Callable
    kwargs: dict[str, Any]

    def __eq__(self, other):
        if not isinstance(other, StreamSubInfo):
            return False
        return self.cb_ref == other.cb_ref

    def __hash__(self) -> int:
        return self.cb_ref.__hash__()


@dataclass
class DirectReadStreamSubInfo(StreamSubInfo):
    stop_event: threading.Event
    thread: threading.Thread

    def __hash__(self) -> int:
        return self.cb_ref.__hash__()


@dataclass
class StreamMessage:
    msg: dict
    callbacks: Iterable[tuple[Callable, dict[str, Any]]]


class StreamSubsEntry(NamedTuple):
    read_id: str
    subs: set[StreamSubInfo]


StreamResponseList = list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]
StreamSubsRegistry = dict[str, StreamSubsEntry]


class StreamSubs:
    def __init__(self) -> None:
        """Manager for stream subscriptions. Since operations often need to be combined,
        use the lock directly at point of call, it is generally not used in the methods."""
        self.lock = threading.RLock()

        self._subs: StreamSubsRegistry = {}
        self._direct_read_subs: dict[
            str, dict[DirectReadStreamSubInfo, DirectReadStreamSubInfo]
        ] = {}
        self.from_start_subs: dict[str, set[StreamSubInfo]] = {}

    @property
    def normal_subs(self):
        return {t: s.subs for t, s in self._subs.items()}

    @property
    def all_topics(self):
        with self.lock:
            from_start_keys = [k for k in self.from_start_subs if self.from_start_subs[k] != set()]
            dr_sub_keys = [k for k in self._direct_read_subs if self._direct_read_subs[k] != set()]
            return list(set((*self._subs.keys(), *dr_sub_keys, *from_start_keys)))

    def topic_ids(self) -> dict[str, str]:
        """Get Redis read Ids for active subscriptions"""
        return {topic: infos.read_id for topic, infos in self._subs.items()}

    def update_normal_ids(self, updated_ids: dict[str, str]):
        for topic, id in updated_ids.items():
            if topic in self._subs:
                self._subs[topic] = StreamSubsEntry(id, self._subs[topic].subs)

    def from_start_topics(self) -> set[str]:
        """Get topics for new `from_start` subscriptions which haven't been read yet"""
        return set(self.from_start_subs.keys())

    def end_id(self, topic: str):
        """Return the last read id for a given topic if given, or "+" """
        return self._subs[topic].read_id if topic in self._subs else "+"

    def move_from_start_to_normal(self, topics_and_end_ids: dict[str, str]):
        if topics_and_end_ids.keys() != self.from_start_subs.keys():
            error_log_with_context(
                f"Mismatch of subs to move! {topics_and_end_ids.keys()=}, {self.from_start_subs.keys()=} Was a lock forgotten?"
            )
        for topic in topics_and_end_ids:
            if topic in self._subs:
                if topics_and_end_ids[topic] != self._subs[topic].read_id:
                    error_log_with_context("Mismatch of ID! Was a lock forgotten?")
                for sub in self.from_start_subs.pop(topic):
                    self._subs[topic].subs.add(sub)  # type: ignore
            else:
                self._subs[topic] = StreamSubsEntry(
                    read_id=topics_and_end_ids[topic], subs=self.from_start_subs.pop(topic)
                )

    def is_already_registered(self, topic: str, new_sub: StreamSubInfo):
        return (
            (topic in self.from_start_subs and new_sub in self.from_start_subs[topic])
            or (topic in self._direct_read_subs and new_sub in self._direct_read_subs[topic])
            or (topic in self._subs and new_sub in self._subs[topic].subs)
        )

    def _check_registered(self, topic: str, new_sub: StreamSubInfo):
        if self.is_already_registered(topic, new_sub):
            raise ValueError(f"Received duplicate subscription for {new_sub=}.")

    def add_direct_listener(self, topic: str, new_sub: DirectReadStreamSubInfo):
        self._check_registered(topic, new_sub)
        if topic not in self._direct_read_subs:
            self._direct_read_subs[topic] = {}
        self._direct_read_subs[topic][new_sub] = new_sub
        new_sub.thread.start()

    def add(self, from_start: bool, last_id: str, topic: str, new_sub: StreamSubInfo):
        """last_id is ignored if from_start is True"""
        self._check_registered(topic, new_sub)
        if from_start:
            if topic in self.from_start_subs:
                subs = self.from_start_subs[topic]
            else:
                subs = set()
                self.from_start_subs[topic] = subs
        else:
            if topic not in self._subs:
                subs = set()
                self._subs[topic] = StreamSubsEntry(read_id=last_id, subs=subs)
            else:
                subs = self._subs[topic].subs
        subs.add(new_sub)

    @staticmethod
    def _kill_direct_stream(sub: DirectReadStreamSubInfo, topic: str):
        sub.stop_event.set()
        sub.thread.join(timeout=1)
        if sub.thread.is_alive():
            error_log_with_context(
                f"RedisConnector direct stream callback thread for {topic=}, {sub.cb_ref=} failed to shutdown"
            )

    def remove(self, topic: str, cb: Callable | None = None) -> bool:
        removed = False
        if cb is None:  # Remove all subs for the given topic
            removed |= bool(self.from_start_subs.pop(topic, False))
            removed |= bool(self._subs.pop(topic, False))
            if (subs := self._direct_read_subs.pop(topic, None)) is not None:
                for sub in subs:
                    self._kill_direct_stream(sub, topic)
                    removed = True
            return removed
        test_subinfo = StreamSubInfo(louie.saferef.safe_ref(cb), {})
        if topic in self.from_start_subs and test_subinfo in self.from_start_subs[topic]:
            self.from_start_subs[topic].remove(test_subinfo)
            removed = True
            if len(self.from_start_subs[topic]) == 0:
                del self.from_start_subs[topic]
        if topic in self._direct_read_subs and test_subinfo in self._direct_read_subs[topic]:
            sub = self._direct_read_subs[topic].pop(test_subinfo)  # type: ignore # hash is the same
            self._kill_direct_stream(sub, topic)
            removed = True
            if len(self._direct_read_subs[topic]) == 0:
                del self._direct_read_subs[topic]
        if topic in self._subs and test_subinfo in self._subs[topic].subs:
            self._subs[topic].subs.remove(test_subinfo)
            removed = True
            if len(self._subs[topic].subs) == 0:
                del self._subs[topic]
        return removed

    def gc_cb_refs(self):
        for topic, entry in list(self._subs.items()):
            for info in list(entry.subs):
                if not info.cb_ref():
                    entry.subs.remove(info)
            if len(self._subs[topic].subs) == 0:
                del self._subs[topic]
        for topic, entry in list(self._direct_read_subs.items()):
            for info in list(entry.keys()):
                if not info.cb_ref():
                    info.stop_event.set()
                    info.thread.join(0.05)
                    if info.thread.is_alive():
                        error_log_with_context(f"Failed to garbage collect in 0.05s {info}")
                    del entry[info]
            if self._direct_read_subs[topic] == {}:
                del self._direct_read_subs[topic]
        for topic, subs in list(self.from_start_subs.items()):
            for info in list(subs):
                if not info.cb_ref():
                    subs.remove(info)
            if len(self.from_start_subs[topic]) == 0:
                del self.from_start_subs[topic]

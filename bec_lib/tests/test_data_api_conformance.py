"""The shipped DataPath conformance kit, run against the shipped template adapter."""

from bec_lib.data_api.testing import REMOTE_SCAN, DataPathConformance, ReferencePath

from .test_data_api_routing import MemoryPath


class TestReferencePathConformance(DataPathConformance):
    def make_path(self, client, data_source, keys):
        return ReferencePath(client, data_source, keys, priority=0)

    def publish(self, path, key, ordinal, value):
        path.send(REMOTE_SCAN, key, ordinal, value)


class TestMemoryPathConformance(DataPathConformance):
    def make_path(self, client, data_source, keys):
        return MemoryPath(client, data_source, keys)

    def publish(self, path, key, ordinal, value):
        path.push(key, ordinal, value)


class ReadOncePath(ReferencePath):
    """An archive-like path: a binding gets the store's state at open time, nothing later."""

    def store(self, key, ordinal, value):
        with self.lock:
            self._points.setdefault((REMOTE_SCAN, key), {})[ordinal] = (value, None)


class TestReadOncePathConformance(DataPathConformance):
    delivers_after_open = False

    def make_path(self, client, data_source, keys):
        return ReadOncePath(client, data_source, keys, priority=0)

    def publish(self, path, key, ordinal, value):
        path.store(key, ordinal, value)

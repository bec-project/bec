import time
from typing import Callable


def wait_until(predicate: Callable[[], bool], timeout_s: float = 0.1):
    # Yes I know this is actually more like retries than a timeout,
    # it's just to make sure the threads have plenty of chances to switch in the test
    elapsed, step = 0.0, timeout_s / 10
    while not predicate():
        time.sleep(step)
        elapsed += step
        if elapsed > timeout_s:
            raise TimeoutError()

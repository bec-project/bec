"""
Throughput guards for the DataAPI pipeline.

These are not micro-benchmarks for tuning — they are regression guards that
fail if the per-emission cost of the alignment engine becomes super-linear
again (the original prototype spent 60+ s of callback CPU on a one-hour scan).
Bounds are generous to stay CI-safe; the printed numbers are the useful part.
"""

import time

from bec_lib.data_api.alignment import Bundle

# pylint: disable=missing-function-docstring


def _run_scenario(n_points: int, fragment: list, emit_every: int) -> tuple[float, int]:
    """Ingest a monitored x + async z scan, emitting every `emit_every` inserts."""
    bundle = Bundle("bench")
    x = bundle.get_series("samx", "samx", "monitored")
    z = bundle.get_series("det", "wave", "async")
    emissions = 0
    start = time.perf_counter()
    for i in range(n_points):
        x.insert(i, float(i), 100.0 + i)
        z.insert(i, fragment, 100.0 + i)
        if i % emit_every == 0:
            update = bundle.build_update("live")
            update.aligned()
            update.axis("device", ("samx", "samx"))
            emissions += 1
    return time.perf_counter() - start, emissions


def test_pipeline_scales_linearly_per_emission():
    fragment = [1.0] * 100
    small, _ = _run_scenario(1000, fragment, emit_every=20)
    large, emissions = _run_scenario(4000, fragment, emit_every=20)
    per_emission_ms = 1000 * large / emissions
    print(
        f"\n[data-api bench] 4000 pts, {emissions} emissions: total {large * 1000:.1f} ms, "
        f"{per_emission_ms:.3f} ms/emission (1000 pts: {small * 1000:.1f} ms)"
    )
    # 4x the points must cost clearly less than the quadratic 16x.
    assert large < small * 10, f"super-linear scaling: {small:.3f}s -> {large:.3f}s"
    # Absolute sanity: an emission (snapshot + aligned + axis) stays sub-10ms.
    assert per_emission_ms < 10, f"emission cost too high: {per_emission_ms:.2f} ms"


def test_unchanged_sources_reuse_cached_snapshots():
    bundle = Bundle("bench")
    x = bundle.get_series("samx", "samx", "monitored")
    z = bundle.get_series("det", "wave", "async")
    for i in range(1000):
        x.insert(i, float(i), 0.0)
        z.insert(i, [1.0] * 10, 0.0)
    first = bundle.build_update("live")
    # Only z changes: the x snapshot must be reused, not rebuilt.
    z.insert(1000, [1.0] * 10, 0.0)
    second = bundle.build_update("live")
    assert second.sources[("samx", "samx")] is first.sources[("samx", "samx")]
    assert second.sources[("det", "wave")] is not first.sources[("det", "wave")]


def test_bulk_history_ingest_is_zero_copy_and_fast():
    """Regression guard for the huge-history GUI freeze: a 1M-point history
    fill must keep the file's numpy array end to end (no per-point Python
    objects) and complete well under the old per-point cost (~450 ms)."""
    import numpy as np

    from bec_lib.data_api.alignment import Bundle

    values = np.random.default_rng(0).random(1_000_000)
    timestamps = np.arange(1_000_000, dtype=float)

    start = time.perf_counter()
    bundle = Bundle("scan_bulk")
    series = bundle.get_series("det", "sig", "monitored")
    assert series.extend_bulk(values, timestamps)
    update = bundle.build_update("history")
    column = update.aligned()[("det", "sig")]
    as_array = np.asarray(column)
    elapsed = time.perf_counter() - start

    # the zero-copy chain is the mechanism - guard it directly
    assert column is values
    assert as_array is values
    assert isinstance(update.aligned_ordinals, np.ndarray)
    # generous absolute bound: measured ~5 ms; the old path took ~450 ms
    assert elapsed < 0.1, f"bulk ingest chain took {elapsed:.3f}s"

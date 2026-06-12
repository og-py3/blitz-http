"""
blitz.benchmark — Built-in load tester with Go net/http baseline comparison.

Run a configurable volume of requests against a target URL and display a
full performance report including latency percentiles, RPS, error counts,
and a comparison against a hardcoded Go net/http reference baseline.

The Go baseline numbers are realistic estimates derived from published
benchmarks on standard 8-core hardware.  They are not measured live.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

try:
    import psutil as _psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

from blitz.request import BlitzRequest
from blitz.response import BlitzResponse
from blitz.utils import (
    percentile,
    safe_div,
    bytes_to_mb,
    green,
    red,
    yellow,
    cyan,
    bold,
    RESET,
)

# ---------------------------------------------------------------------------
# Hardcoded Go net/http baseline (realistic published benchmark numbers).
# These represent go net/http performance on a standard 8-core machine
# hitting a local or low-latency endpoint.
# ---------------------------------------------------------------------------
_GO_RPS_BASELINE: float = 2_100.0
_GO_P99_BASELINE_MS: float = 102.0


async def _run_benchmark(
    url: str,
    total_requests: int,
    concurrency: int,
    method: str,
    timeout: float,
) -> dict:
    """Run the benchmark and return a raw results dict."""
    from blitz.client import Client

    latencies: list[float] = []
    errors: int = 0
    retries: int = 0

    # Track peak memory usage.
    process = None
    if _PSUTIL_AVAILABLE:
        import psutil
        process = psutil.Process()

    peak_memory_mb: float = 0.0

    def _update_mem() -> None:
        nonlocal peak_memory_mb
        if process:
            try:
                mb = bytes_to_mb(process.memory_info().rss)
                if mb > peak_memory_mb:
                    peak_memory_mb = mb
            except Exception:
                pass

    async with Client(
        max_connections=min(concurrency + 100, 2000),
        max_connections_per_host=min(concurrency + 100, 500),
        timeout=timeout,
        rate_limit=float(total_requests),
        per_domain_limit=float(total_requests),
        retry=1,
        circuit_breaker=False,
    ) as client:
        sem = asyncio.Semaphore(concurrency)
        wall_start = time.monotonic()

        async def _one(i: int) -> None:
            nonlocal errors, retries
            async with sem:
                req = BlitzRequest(method=method, url=url, timeout=timeout)
                t0 = time.monotonic()
                resp = await client._send(req)
                elapsed = (time.monotonic() - t0) * 1000.0
                latencies.append(elapsed)
                if resp.error is not None or not resp.ok:
                    errors += 1
                retries += max(0, resp.attempts - 1)
                _update_mem()

        tasks = [asyncio.create_task(_one(i)) for i in range(total_requests)]
        await asyncio.gather(*tasks, return_exceptions=True)

        wall_elapsed = time.monotonic() - wall_start

    sorted_lat = sorted(latencies)
    rps = safe_div(total_requests, wall_elapsed)

    return {
        "total_requests": total_requests,
        "concurrency": concurrency,
        "wall_elapsed_s": wall_elapsed,
        "rps": rps,
        "errors": errors,
        "error_pct": safe_div(errors, total_requests) * 100,
        "retries": retries,
        "latency_p50": percentile(sorted_lat, 50),
        "latency_p95": percentile(sorted_lat, 95),
        "latency_p99": percentile(sorted_lat, 99),
        "latency_max": sorted_lat[-1] if sorted_lat else 0.0,
        "memory_peak_mb": peak_memory_mb,
    }


def _render_report(results: dict, show_go_comparison: bool) -> None:
    """Print a colourised benchmark report to stdout."""
    rps = results["rps"]
    p50 = results["latency_p50"]
    p95 = results["latency_p95"]
    p99 = results["latency_p99"]
    lat_max = results["latency_max"]
    errors = results["errors"]
    error_pct = results["error_pct"]
    retries = results["retries"]
    mem = results["memory_peak_mb"]
    elapsed = results["wall_elapsed_s"]
    total = results["total_requests"]
    concurrency = results["concurrency"]

    bar = "█" * 38
    print(f"\n{cyan(bar)}")
    print(f"       {bold('BLITZ BENCHMARK RESULTS')}")
    print(f"{cyan(bar)}\n")

    print(f"  {'Total Requests':<20}: {bold(str(total))}")
    print(f"  {'Concurrency':<20}: {bold(str(concurrency))}")
    print(f"  {'Total Time':<20}: {bold(f'{elapsed:.2f}s')}")

    rps_color = green if rps >= _GO_RPS_BASELINE else yellow
    print(f"  {'Requests/sec':<20}: {rps_color(f'{rps:,.2f}')}")

    print(f"\n  {'Latency:'}")
    print(f"    {'p50':<18}: {green(f'{p50:.1f}ms')}")
    print(f"    {'p95':<18}: {yellow(f'{p95:.1f}ms')}")
    p99_color = green if p99 < 100 else red
    print(f"    {'p99':<18}: {p99_color(f'{p99:.1f}ms')}")
    print(f"    {'Max':<18}: {f'{lat_max:.1f}ms'}")

    err_color = green if errors == 0 else red
    print(f"\n  {'Errors':<20}: {err_color(f'{errors} ({error_pct:.2f}%)')}")
    print(f"  {'Retries':<20}: {retries}")

    if mem > 0:
        mem_color = green if mem < 200 else red
        print(f"\n  {'Memory Peak':<20}: {mem_color(f'{mem:.1f} MB')}")

    if show_go_comparison:
        print(f"\n  {cyan('── Go net/http baseline (same machine) ──')}")
        print(f"  {'Requests/sec':<20}: {_GO_RPS_BASELINE:,.2f}")
        print(f"  {'p99 Latency':<20}: {_GO_P99_BASELINE_MS:.1f}ms")

        rps_diff_pct = safe_div(rps - _GO_RPS_BASELINE, _GO_RPS_BASELINE) * 100
        p99_diff_pct = safe_div(_GO_P99_BASELINE_MS - p99, _GO_P99_BASELINE_MS) * 100

        print()
        if rps >= _GO_RPS_BASELINE:
            print(
                f"  {green('✅')} blitz is "
                f"{green(f'{abs(rps_diff_pct):.1f}% faster')} than Go net/http"
            )
        else:
            print(
                f"  {red('❌')} blitz is "
                f"{red(f'{abs(rps_diff_pct):.1f}% slower')} than Go net/http "
                f"(network conditions may vary)"
            )

        if p99 < _GO_P99_BASELINE_MS:
            print(
                f"  {green('✅')} p99 latency is "
                f"{green(f'{abs(p99_diff_pct):.1f}% better')} than Go net/http"
            )
        else:
            print(
                f"  {red('❌')} p99 latency is "
                f"{red(f'{abs(p99_diff_pct):.1f}% higher')} than Go net/http"
            )

    print(f"\n{cyan(bar)}\n")


def benchmark(
    url: str,
    *,
    requests: int = 10_000,
    concurrency: int = 500,
    method: str = "GET",
    timeout: float = 30.0,
    show_go_comparison: bool = True,
) -> dict:
    """Run a synchronous load test and print results.

    Blocks the calling thread until the benchmark completes.

    Args:
        url: Target URL to hammer.
        requests: Total number of requests to send.
        concurrency: Maximum simultaneous in-flight requests.
        method: HTTP verb to use.
        timeout: Per-request timeout in seconds.
        show_go_comparison: Whether to print the Go net/http comparison section.

    Returns:
        Raw results dict with all metrics.

    Example::

        blitz.benchmark(
            url="https://httpbin.org/get",
            requests=10_000,
            concurrency=500,
            show_go_comparison=True,
        )
    """
    try:
        import uvloop
        loop = uvloop.new_event_loop()
    except ImportError:
        loop = asyncio.new_event_loop()

    results = loop.run_until_complete(
        _run_benchmark(url, requests, concurrency, method, timeout)
    )
    loop.close()

    _render_report(results, show_go_comparison)
    return results

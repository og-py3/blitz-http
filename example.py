"""
example.py — blitz end-to-end demonstration.

This script:
  1. Creates a blitz.Client with a full production configuration.
  2. Fires 1,000 GET requests to httpbin.org/get with live stats streaming.
  3. Prints a full benchmark summary at the end.

Usage:
    pip install -r requirements.txt
    python example.py
"""

import asyncio
import sys

import blitz


async def main() -> None:
    """Run the full blitz demonstration."""

    print("\n" + "=" * 60)
    print("  blitz — High-Performance Async HTTP Client Demo")
    print("=" * 60 + "\n")

    # ----------------------------------------------------------------
    # 1. Create a fully-configured client.
    # ----------------------------------------------------------------
    client = blitz.Client(
        # Worker config
        min_workers=8,
        max_workers=64,

        # Connection config
        max_connections=500,
        max_connections_per_host=100,
        keepalive_timeout=30.0,
        connection_timeout=5.0,

        # Request config
        timeout=15.0,
        follow_redirects=True,
        max_redirects=5,

        # Rate limiting — generous for a demo
        rate_limit=5000.0,
        per_domain_limit=5000.0,
        burst_multiplier=2.0,

        # Retry config
        retry=3,
        retry_strategy="jitter",
        retry_min_wait=0.1,
        retry_max_wait=10.0,

        # Circuit breaker
        circuit_breaker=True,
        cb_failure_threshold=10,
        cb_recovery_timeout=30.0,

        # HTTP config
        http2=True,
        verify_ssl=True,
        headers={"User-Agent": "blitz/1.0 (demo)"},
    )

    # ----------------------------------------------------------------
    # 2. Start live stats display.
    # ----------------------------------------------------------------
    await client.start()
    client.live(interval=1.0)

    # ----------------------------------------------------------------
    # 3. Fire 1,000 requests concurrently.
    # ----------------------------------------------------------------
    TARGET_URL = "https://httpbin.org/get"
    N_REQUESTS = 1_000
    CONCURRENCY = 100

    print(f"Sending {N_REQUESTS:,} GET requests to {TARGET_URL}")
    print(f"Concurrency: {CONCURRENCY} | Live stats updating every second...\n")

    urls = [TARGET_URL] * N_REQUESTS
    requests = [blitz.Request("GET", url, tags={"index": i}) for i, url in enumerate(urls)]

    responses = await client.batch(requests, concurrency=CONCURRENCY)

    # Clear the live-stats line.
    print()

    # ----------------------------------------------------------------
    # 4. Summarise results.
    # ----------------------------------------------------------------
    successes = sum(1 for r in responses if r.ok)
    errors = sum(1 for r in responses if not r.ok)
    latencies = [r.latency_ms for r in responses if r.latency_ms > 0]

    print("\n" + "=" * 60)
    print("  Results Summary")
    print("=" * 60)
    print(f"  Requests sent  : {N_REQUESTS:,}")
    print(f"  Successes      : {successes:,}")
    print(f"  Errors         : {errors:,}")
    if latencies:
        latencies.sort()
        from blitz.utils import percentile
        print(f"  p50 latency    : {percentile(latencies, 50):.1f}ms")
        print(f"  p95 latency    : {percentile(latencies, 95):.1f}ms")
        print(f"  p99 latency    : {percentile(latencies, 99):.1f}ms")
        print(f"  Max latency    : {latencies[-1]:.1f}ms")
    print()

    await client.close()

    # ----------------------------------------------------------------
    # 5. Run the built-in benchmark (200 requests for speed).
    # ----------------------------------------------------------------
    print("=" * 60)
    print("  Running built-in benchmark...")
    print("=" * 60 + "\n")

    blitz.benchmark(
        url=TARGET_URL,
        requests=500,
        concurrency=100,
        method="GET",
        show_go_comparison=True,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[interrupted]")
        sys.exit(0)

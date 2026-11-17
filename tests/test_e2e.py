"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
            },
        )
        r.raise_for_status()


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


def test_reserved_sidecar_names_rejected(proxy):
    """A package must not be able to shadow cache sidecars of another key."""
    with client() as c:
        for path in (f"{PKG_SMALL}.meta", f"{PKG_SMALL}.part-deadbeef",
                     f"{PKG_SMALL}.meta.tmp-deadbeef"):
            r = c.get(f"{proxy.url}/packages/{path}")
            assert r.status_code == 400, path
            r = c.post(f"{proxy.url}/purge/{path}")
            assert r.status_code == 400, path


# --------------------------------------------------------------------- #
# purge/fill interleaving: old generation draining, new generation live  #
# --------------------------------------------------------------------- #

def _change_content(origin_url: str, path: str) -> dict:
    with client() as c:
        r = c.post(f"{origin_url}/control/content/{path}")
        r.raise_for_status()
        return r.json()


def _slow_reader(proxy_url: str, path: str, *, read_chunk: int = 32 * 1024,
                 pause: float = 0.12, result: dict | None = None):
    """Download slowly so the reader is still draining after the fill ends."""

    def run():
        with client(timeout_read=300).stream(
            "GET", f"{proxy_url}/packages/{path}"
        ) as r:
            buf = []
            for chunk in r.iter_bytes(read_chunk):
                buf.append(chunk)
                time.sleep(pause)
            if result is not None:
                result["body"] = b"".join(buf)

    return threading.Thread(target=run)


def test_old_reader_exit_does_not_delete_new_generation(proxy, origin):
    """The reported defect: purge mid-fill + new fill. The old reader must
    read the pre-purge bytes to the end, while the new generation's cache
    file/meta stay intact after that old reader detaches, and remain a HIT
    (including across a restart) instead of failing."""
    # ~0.8s fill for 1MiB; the old reader then takes ~4s to drain.
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        old_sha = origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]

        result: dict = {}
        reader_a = _slow_reader(proxy.url, PKG_SMALL, result=result)
        reader_a.start()
        time.sleep(0.3)  # A is mid-fill, well before the fill completes

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True
        new = _change_content(origin.url, PKG_SMALL)
        assert new["sha256"] != old_sha
        set_behavior(origin.url)  # new generation fills at full speed

        # Trigger the new generation while the old one is still draining.
        with client() as c:
            r_new = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r_new.status_code == 200
        assert sha256(r_new.content) == new["sha256"], "new download got stale bytes"
        assert r_new.headers["x-cache"] == "MISS"

        reader_a.join(120)
        assert not reader_a.is_alive()
        old_body = result["body"]
        assert len(old_body) == MIB
        assert sha256(old_body) == old_sha, "old download did not get pre-purge bytes"

        # Old reader is gone; the new generation must not have been reaped.
        assert (proxy.cache_dir / PKG_SMALL).is_file()
        assert (proxy.cache_dir / f"{PKG_SMALL}.meta").is_file()
        assert list(proxy.cache_dir.glob(f"{PKG_SMALL}.part-*")) == []

        fetches_before = origin_stats(origin.url)["paths"][PKG_SMALL]
        with client() as c:
            r_hit = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r_hit.headers["x-cache"] == "HIT"
        assert sha256(r_hit.content) == new["sha256"]
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == fetches_before

        # The new generation must also survive a restart (served from disk).
        proxy.restart()
        with client() as c:
            r_boot = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r_boot.headers["x-cache"] == "HIT"
        assert sha256(r_boot.content) == new["sha256"]
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == fetches_before
    finally:
        set_behavior(origin.url)


def test_two_old_readers_drain_without_touching_new_generation(proxy, origin):
    """Several readers on the purged generation: the last one to leave must
    still not delete the newer generation's files, and existing new readers
    attached at publish time keep their own bytes."""
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        old_sha = origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]
        results: list[dict] = [{} for _ in range(3)]
        readers = [
            _slow_reader(proxy.url, PKG_SMALL, pause=0.08, result=results[0]),
            _slow_reader(proxy.url, PKG_SMALL, pause=0.16, result=results[1]),
            _slow_reader(proxy.url, PKG_SMALL, pause=0.24, result=results[2]),
        ]
        for t in readers:
            t.start()
            time.sleep(0.1)
        time.sleep(0.2)

        with client() as c:
            assert c.post(f"{proxy.url}/purge/{PKG_SMALL}").json()["was_cached"]
        new = _change_content(origin.url, PKG_SMALL)
        set_behavior(origin.url)

        body_new = download_bytes(proxy.url, PKG_SMALL)
        assert sha256(body_new) == new["sha256"]

        for t in readers:
            t.join(120)
            assert not t.is_alive()
        for res in results:
            assert len(res["body"]) == MIB
            assert sha256(res["body"]) == old_sha

        # Exactly two origin fetches total: the original fill and the refill.
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
        assert (proxy.cache_dir / PKG_SMALL).is_file()
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == new["sha256"]
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    finally:
        set_behavior(origin.url)


def test_upstream_abort_during_new_fill_after_purge_caches_nothing(proxy, origin):
    """Generation interleaving must not weaken failure semantics: while an
    old reader drains a purged generation, a new fill that aborts upstream
    fails its own readers and leaves no half package or meta behind. A later
    retry gets fresh bytes and becomes a HIT."""
    set_behavior(origin.url, delay_ms=40, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        old_sha = origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]
        result: dict = {}
        reader_old = _slow_reader(proxy.url, PKG_SMALL, pause=0.1, result=result)
        reader_old.start()
        time.sleep(0.25)

        with client() as c:
            assert c.post(f"{proxy.url}/purge/{PKG_SMALL}").json()["was_cached"]

        # New generation: abort mid-body.
        set_behavior(origin.url, delay_ms=20, chunk_size=64 * 1024,
                     truncate=300_000)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(3) as ex:
            list(ex.map(lambda _: expect_failure(), range(3)))
        # Only the original fill and the aborted refill hit the origin: the
        # failing generation is shared, not retried per client.
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2

        reader_old.join(120)
        assert sha256(result["body"]) == old_sha  # old generation unaffected

        # No half package survived anywhere.
        leftovers = sorted(p.name for p in proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
        with client() as c:
            assert c.get(f"{proxy.url}/cache/status").json().get(PKG_SMALL) is None

        # Healthy origin again: next request refills (one single flight).
        set_behavior(origin.url)
        new = _change_content(origin.url, PKG_SMALL)
        with ThreadPoolExecutor(4) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(4)))
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 3
        assert all(sha256(b) == new["sha256"] for b in bodies)
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == new["sha256"]
    finally:
        set_behavior(origin.url)


def _cache_status(proxy_url: str) -> dict:
    with client() as c:
        r = c.get(f"{proxy_url}/cache/status")
        r.raise_for_status()
        return r.json()


def _wait_for(cond, timeout: float = 30.0, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise AssertionError("condition not reached in time")


def test_purge_hammer_while_filling_keeps_single_flight_and_clean_disk(proxy, origin):
    """Repeated purge/content changes during fills: every response must match
    one of the origin contents, no download may fail, origin fetches stay
    bounded (one fill per generation), and disk ends with only complete
    files for the winning generation."""
    set_behavior(origin.url, delay_ms=25, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        seen = {origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]}
        errors: list[BaseException] = []
        bodies: list[bytes] = []

        def reader():
            try:
                bodies.append(download_bytes(proxy.url, PKG_SMALL))
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        with ThreadPoolExecutor(12) as ex:
            futures = [ex.submit(reader) for _ in range(12)]
            for i in range(3):
                time.sleep(0.12)
                with client() as c:
                    c.post(f"{proxy.url}/purge/{PKG_SMALL}")
                seen.add(_change_content(origin.url, PKG_SMALL)["sha256"])
                set_behavior(origin.url, delay_ms=25, chunk_size=64 * 1024)
            for f in futures:
                f.result(120)

        assert not errors, errors
        assert len(bodies) == 12
        assert all(len(b) == MIB for b in bodies)
        assert all(sha256(b) in seen for b in bodies)

        # Single-flight per generation: never more origin fetches than
        # generations requested (initial + 3 content changes = 4).
        fetches = origin_stats(origin.url)["paths"][PKG_SMALL]
        assert fetches <= 4, f"too many origin fetches: {fetches}"
        assert fetches < 12

        leftovers = sorted(p.name for p in proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert all(".part-" not in n and ".final-" not in n for n in leftovers), leftovers

        # Settle: the last purge may legitimately arrive after all readers
        # finished, so the first request after it may still MISS; from then on
        # the final generation must be a HIT and survive a restart.
        set_behavior(origin.url)
        final_sha = origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]
        _wait_for(lambda: not _cache_status(proxy.url), timeout=10)
        with client() as c:
            r_settle = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert sha256(r_settle.content) == final_sha
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == final_sha
        before = origin_stats(origin.url)["paths"][PKG_SMALL]
        proxy.restart()
        with client() as c:
            r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r2.headers["x-cache"] == "HIT"
        assert sha256(r2.content) == final_sha
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == before
    finally:
        set_behavior(origin.url)


def test_restart_after_purge_before_old_reader_exits_refills(proxy, origin):
    """Purge must be durable: if the proxy restarts while a slow reader is
    still draining the purged generation, the next request may not adopt the
    old bytes as a cache hit. It refills the current origin content."""
    origin_reset(origin.url)
    old = download_bytes(proxy.url, PKG_BIG)
    old_sha = sha256(old)
    new = _change_content(origin.url, PKG_BIG)

    started = threading.Event()

    def slow_old():
        try:
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                assert r.headers["x-cache"] == "HIT"
                for _ in r.iter_bytes(64 * 1024):
                    started.set()
                    time.sleep(0.02)
        except httpx.HTTPError:
            pass  # the restart under test cuts this connection; expected

    reader = threading.Thread(target=slow_old)
    reader.start()
    assert started.wait(30)
    _wait_for(lambda: _cache_status(proxy.url).get(PKG_BIG, {}).get("readers", 0) >= 1)

    with client() as c:
        assert c.post(f"{proxy.url}/purge/{PKG_BIG}").json()["was_cached"]
    # The canonical cache file is gone the moment purge returns.
    assert not (proxy.cache_dir / PKG_BIG).exists()

    # Restart the proxy while the old reader is still attached (its client
    # gets cut off by the restart; that's expected).
    proxy.restart()
    reader.join(60)

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
    assert sha256(r.content) == new["sha256"], "purged bytes were adopted after restart"
    assert r.headers["x-cache"] == "MISS"
    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_BIG}")
    assert r2.headers["x-cache"] == "HIT"
    assert sha256(r2.content) == new["sha256"]


def test_nested_key_generation_interleaving(proxy, origin):
    """Same interleaving for a nested path key: sidecars live next to the
    nested file, and an old draining generation must not disturb the new."""
    key = f"team/sub/{PKG_SMALL}"
    pkg_dir = origin.dir / "team" / "sub"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    body0 = os.urandom(MIB)
    (pkg_dir / PKG_SMALL).write_bytes(body0)
    # Origin hashes packages at boot only; restart it so it serves/digests
    # this nested file, then swap content through the control API.
    with client() as c:
        c.post(f"{origin.url}/control/content/{key}")  # creates it too
    with client() as c:
        old = c.get(f"{origin.url}/control/stats").json()["packages"][key]

    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}
        reader = _slow_reader(proxy.url, key, result=result)
        reader.start()
        time.sleep(0.25)
        with client() as c:
            assert c.post(f"{proxy.url}/purge/{key}").json()["was_cached"]
        new = _change_content(origin.url, key)
        assert new["sha256"] != old["sha256"]
        set_behavior(origin.url)

        body_new = download_bytes(proxy.url, key)
        assert sha256(body_new) == new["sha256"]

        reader.join(120)
        assert sha256(result["body"]) == old["sha256"]
        cache_file = proxy.cache_dir / "team" / "sub" / PKG_SMALL
        assert cache_file.is_file()
        assert cache_file.with_name(PKG_SMALL + ".meta").is_file()
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{key}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == new["sha256"]
    finally:
        set_behavior(origin.url)


def test_slow_hit_reader_on_purged_complete_generation_keeps_new_cache(proxy, origin):
    """Old generation was already complete and cached, with a slow reader
    still downloading it (attached in the proxy) when purge + a new fill
    happen. When that old reader exits it must not unlink the new
    generation's file/meta."""
    origin_reset(origin.url)
    old = download_bytes(proxy.url, PKG_BIG)
    old_sha = sha256(old)
    new = _change_content(origin.url, PKG_BIG)
    assert new["sha256"] != old_sha

    started = threading.Event()
    result: dict = {}

    def slow_hit():
        with client(timeout_read=300).stream(
            "GET", f"{proxy.url}/packages/{PKG_BIG}"
        ) as r:
            assert r.headers["x-cache"] == "HIT"
            buf = []
            for chunk in r.iter_bytes(64 * 1024):
                buf.append(chunk)
                started.set()
                time.sleep(0.02)  # ~10s for 32MiB, slower than loopback buffers
            result["body"] = b"".join(buf)

    reader = threading.Thread(target=slow_hit)
    reader.start()
    assert started.wait(30), "old HIT reader never started streaming"
    # Wait until the proxy itself still has this reader attached.
    _wait_for(lambda: _cache_status(proxy.url).get(PKG_BIG, {}).get("readers", 0) >= 1)

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_BIG}")
    assert pr.json()["was_cached"] is True

    body_new = download_bytes(proxy.url, PKG_BIG)  # new generation
    assert sha256(body_new) == new["sha256"]
    assert reader.is_alive(), "need the old reader to outlive the new fill"

    reader.join(300)
    assert not reader.is_alive()
    assert len(result["body"]) == 32 * MIB
    assert sha256(result["body"]) == old_sha, "old HIT reader got replaced bytes"

    assert (proxy.cache_dir / PKG_BIG).is_file()
    assert (proxy.cache_dir / f"{PKG_BIG}.meta").is_file()
    fetches = origin_stats(origin.url)["paths"][PKG_BIG]
    assert fetches == 2
    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
    assert r.headers["x-cache"] == "HIT"
    assert sha256(r.content) == new["sha256"]
    assert origin_stats(origin.url)["paths"][PKG_BIG] == fetches

    proxy.restart()
    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_BIG}")
    assert r2.headers["x-cache"] == "HIT"
    assert sha256(r2.content) == new["sha256"]
    assert origin_stats(origin.url)["paths"][PKG_BIG] == fetches


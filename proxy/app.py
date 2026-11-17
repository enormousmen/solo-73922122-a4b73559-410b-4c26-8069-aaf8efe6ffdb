"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Every generation pins its own data path (``<key>.part-<gen>``). Publishing a
  completed generation adds a hard link at the canonical ``<key>`` path but
  keeps the generation link alive for as long as the generation has readers.
  Readers therefore always read the exact bytes of the generation they
  attached to, even if a purge and a newer fill replace ``<key>`` meanwhile.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered. Reaping an old
  generation deletes only files it owns: its own ``.part-<gen>`` link and the
  canonical file/meta only while they still are the exact inode/version this
  generation published. A newer generation's cache and metadata survive the
  last old reader leaving.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*``/``.final-*`` temp links
  and keeps complete packages, which are served again after a restart.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import BinaryIO

import aiohttp
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")
# Generated side files only; package keys themselves are never hex-only
# suffixes after ".part-"/".final-", so real packages are left alone.
_TEMP_RE = re.compile(r"\.(?:part|final|meta\.tmp)-[0-9a-f]+\Z")

FileId = tuple[int, int]


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal.

    The cache stores sidecars next to each key (``<key>.meta``) and transient
    generation links (``<key>.part-<gen>``, ``<key>.final-<gen>``,
    ``<key>.meta.tmp-<gen>``). Any segment using those reserved suffixes is
    rejected, because a nested key's directory could otherwise collide with
    another key's sidecar.
    """
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
        if seg.endswith(".meta") or _TEMP_RE.search(seg):
            raise ValueError(f"reserved package path segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _file_id(path: Path) -> FileId | None:
    """Identity of the inode currently at ``path`` (None if absent)."""
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, linked into place)
                 \-> closed             (purged mid-fill; fill finished, part
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)

    ``part_is_link`` says whether ``part_path`` exists as a readable name for
    this generation's bytes. It starts true while filling, stays true after a
    hard-link publish, and is false only if the filesystem refused the link
    (fallback: readers then use ``final_path``).
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        # Ownership tokens for the canonical paths. Reaping may only remove a
        # file whose identity still matches what this generation wrote.
        self.published_ident: FileId | None = None
        self.meta_ident: FileId | None = None
        self.meta_gen: str | None = None
        self.part_is_link = True
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    @property
    def data_path(self) -> Path:
        """The path readers of this generation open (never a newer one)."""
        return self.part_path if self.part_is_link else self.final_path

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        # Current generation per key (what new requests attach to).
        self.entries: dict[str, Entry] = {}
        # Every generation that still owns files or readers, including
        # generations that were purged and are draining.
        self.live: set[Entry] = set()
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        ident = (st.st_dev, st.st_ino)
        # The canonical file may still be a purged generation that is only
        # kept alive for its draining readers. That is NOT a cache hit: a new
        # request must refill from the origin. Never adopt bytes that some
        # live generation still owns.
        if any(live.published_ident == ident for live in self.live):
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            meta = {}
        # A meta file whose recorded size or data identity disagrees with the
        # file on disk is stale (e.g. crash between publishing data and meta
        # for the next generation); don't trust it.
        ident = [st.st_dev, st.st_ino]
        if meta.get("size") not in (None, st.st_size):
            meta = {}
        elif meta.get("data_id") is not None and list(meta["data_id"]) != ident:
            meta = {}
        e.meta = meta
        e.meta.setdefault("content_type", "application/octet-stream")
        e.meta_gen = meta.get("gen")
        e.meta_ident = _file_id(e.meta_path)
        e.published_ident = (st.st_dev, st.st_ino)
        # Pinned lazily on attach: an idle cached package needs no extra name.
        e.part_is_link = False
        self.entries[key] = e
        self.live.add(e)
        return e

    def _pin_generation_locked(self, e: Entry) -> None:
        """Give a complete generation its own path before handing it out.

        Readers must be able to finish from the exact inode they started on,
        even if a purge + refill replaces ``<key>`` later. A hard link keeps
        the old inode reachable under ``.part-<gen>`` at no extra disk cost.
        """
        if e.state != "complete" or e.part_is_link or e.readers > 0:
            return
        try:
            os.link(e.final_path, e.part_path)
            e.part_is_link = True
            e.published_ident = _file_id(e.part_path)
            e.meta_ident = _file_id(e.meta_path)
            self.live.add(e)
        except OSError:
            # Filesystem without hard links: readers use the canonical path;
            # purge still defers deletion while this entry has readers.
            e.part_is_link = False
            e.published_ident = _file_id(e.final_path)

    async def attach(self, key: str) -> tuple[Entry, BinaryIO | None]:
        """Return the current generation plus an already-open fd if possible.

        For a complete/closed generation the file is opened while the cache
        lock is held, so a concurrent purge cannot remove the generation
        between counting the reader and opening its bytes. Filling
        generations are opened later, after the part file exists; their file
        stays alive as long as ``readers > 0``.
        """
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                self.live.add(e)
                e.fill_task = asyncio.create_task(self._fill(e))
            else:
                self._pin_generation_locked(e)
            e.readers += 1
            fd: BinaryIO | None = None
            if e.state != "filling":
                fd = self._open_current(e)  # under lock: cannot be reaped
            return e, fd

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader.

        Files owned by *other* generations (an older one still draining, or a
        newer one created by a concurrent request) are never touched.
        """
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                if e.state != "filling":
                    if not e.part_is_link:
                        # No hard-link support: rename the canonical file to
                        # this generation's own name. Already-open reader file
                        # descriptors keep working, and the canonical path is
                        # gone so a restart can't adopt purged bytes.
                        try:
                            os.replace(e.final_path, e.part_path)
                            e.part_is_link = True
                        except OSError:
                            pass
                    if e.part_is_link:
                        # Readers keep their own generation link, so the
                        # canonical name can go right away: after purge
                        # returns, nothing (including a restart) may mistake
                        # these bytes for a cache hit.
                        self._unlink_owned_final_locked(e)
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk,
            # but leave files still owned by live (draining) generations.
            existed = False
            final = CACHE_DIR / key
            owned_data = {live.published_ident for live in self.live if live.key == key}
            owned_meta = {live.meta_ident for live in self.live if live.key == key}
            if _file_id(final) is not None and _file_id(final) not in owned_data:
                _unlink_quiet(final)
                existed = True
            stale_meta = CACHE_DIR / f"{key}.meta"
            meta_ident = _file_id(stale_meta)
            if meta_ident is not None and meta_ident not in owned_meta:
                _unlink_quiet(stale_meta)
                existed = True
            protected = {
                live.part_path
                for live in self.live
                if live.key == key and live.part_is_link
            }
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    if p not in protected:
                        _unlink_quiet(p)
                        existed = True
                for pattern in (".final-*", ".meta.tmp-*"):
                    for p in final.parent.glob(final.name + pattern):
                        _unlink_quiet(p)
                        existed = True
            except OSError:
                pass
            return existed

    def _meta_still_ours(self, e: Entry) -> bool:
        """True only if the canonical meta file is the version we wrote."""
        if e.meta_ident is None or _file_id(e.meta_path) != e.meta_ident:
            return False
        try:
            data = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            # We cannot re-verify an unreadable file; the inode we recorded
            # must still be the one at the path (checked above).
            return e.meta_gen is None
        if "gen" in data:
            return data.get("gen") == e.meta_gen
        if "data_id" in data:
            return data.get("data_id") == (
                list(e.published_ident) if e.published_ident else None
            )
        # Legacy/foreign meta without an ownership stamp: the inode is still
        # the one we adopted, and any newer publish replaces the inode.
        return True

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete this generation's files once no readers remain.

        Ownership checks are essential: by the time an old generation drains,
        ``<key>`` and ``<key>.meta`` may already describe a newer generation.
        """
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            if e.part_is_link:
                _unlink_quiet(e.part_path)  # never keep a half package
            self.live.discard(e)
            return
        if e.part_is_link:
            _unlink_quiet(e.part_path)
            e.part_is_link = False
        if e.purged:
            self._unlink_owned_final_locked(e)
        self.live.discard(e)

    def _unlink_owned_final_locked(self, e: Entry) -> None:
        """Remove canonical data/meta only while they still belong to ``e``."""
        # The retained part link keeps this generation's inode alive, so a
        # newer publish necessarily has a different inode.
        if (
            e.published_ident is not None
            and _file_id(e.final_path) == e.published_ident
        ):
            _unlink_quiet(e.final_path)
        if self._meta_still_ours(e):
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    def _publish_locked(self, e: Entry) -> None:
        """Publish data, then metadata, both after the bytes are complete.

        Data is linked in first (one inode at both ``.part-<gen>`` and
        ``<key>``) so readers keep their generation. Metadata records the data
        inode and generation; if we crash in between, startup discards the
        mismatched meta instead of advertising the wrong package.
        """
        e.meta["gen"] = e.gen
        link_tmp = CACHE_DIR / f"{e.key}.final-{e.gen}"
        try:
            # Two names, one inode: readers keep reading .part-<gen> while
            # <key> can be atomically replaced by a newer generation.
            os.link(e.part_path, link_tmp)
            os.replace(link_tmp, e.final_path)
            e.part_is_link = True
        except OSError:
            _unlink_quiet(link_tmp)
            os.replace(e.part_path, e.final_path)  # fallback: no hard links
            e.part_is_link = False
        e.published_ident = _file_id(e.final_path)
        if e.published_ident is not None:
            e.meta["data_id"] = list(e.published_ident)

        meta_tmp = CACHE_DIR / f"{e.key}.meta.tmp-{e.gen}"
        try:
            meta_tmp.write_text(json.dumps(e.meta))
            os.replace(meta_tmp, e.meta_path)
            e.meta_ident = _file_id(e.meta_path)
            e.meta_gen = e.gen
        except OSError:
            _unlink_quiet(meta_tmp)
            # Don't leave a previous generation's meta next to new data.
            _unlink_quiet(e.meta_path)
            e.meta_ident = None
            e.meta_gen = None

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: keep the part file for current readers
                    # only; never register this generation.
                    e.state = "closed"
                else:
                    self._publish_locked(e)
                    e.state = "complete"
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry) -> BinaryIO:
        for path in (e.data_path, e.part_path, e.final_path):
            try:
                return open(path, "rb")
            except FileNotFoundError:
                continue
        raise FileNotFoundError(f"no readable data for generation {e.gen}")

    async def stream(self, e: Entry, f: BinaryIO | None = None):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        if f is None:
            f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await self.detach(e)


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills/temp links and orphan meta, keep packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if _TEMP_RE.search(name):
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    entry, fd = await cache.attach(key)
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        if fd is not None:
            fd.close()
        await cache.detach(entry)
        raise

    if entry.state == "failed":
        if fd is not None:
            fd.close()
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry, fd),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}

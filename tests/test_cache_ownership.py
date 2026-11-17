"""Unit-level checks for generation ownership bookkeeping.

These do not start HTTP servers: they drive Cache/Entry directly against a
temporary CACHE_DIR. They cover the filesystem fallback (no hard links) and
the rule that only the owning generation may delete canonical files.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "proxy"))

import app as proxy_app  # noqa: E402


@pytest.fixture()
def cache_mod(tmp_path, monkeypatch):
    monkeypatch.setattr(proxy_app, "CACHE_DIR", tmp_path)
    cache = proxy_app.Cache()
    yield proxy_app, cache, tmp_path


def _write_complete(cache_mod, key: str, body: bytes, gen: str) -> object:
    mod, cache, root = cache_mod
    final = root / key
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(body)
    st = final.stat()
    meta = {
        "content_type": "application/octet-stream",
        "sha256": "deadbeef",
        "size": len(body),
        "gen": gen,
        "data_id": [st.st_dev, st.st_ino],
    }
    (root / f"{key}.meta").write_text(json.dumps(meta))
    e = mod.Entry(key)
    e.gen = gen
    e.state = "complete"
    e.size = len(body)
    e.written = len(body)
    e.headers_known = True
    e.meta = meta
    e.published_ident = (st.st_dev, st.st_ino)
    e.meta_ident = mod._file_id(root / f"{key}.meta")
    e.meta_gen = gen
    e.part_is_link = False
    cache.entries[key] = e
    cache.live.add(e)
    return e


def test_no_hardlink_fallback_purge_defers_and_removes(cache_mod, monkeypatch):
    """Without hard links, the canonical file is renamed into the generation
    name on purge; readers keep their already-open descriptor and the last
    detach removes the generation file."""
    mod, cache, root = cache_mod
    e = _write_complete(cache_mod, "pkg.bin", b"abcdef", "genA")

    def no_link(*a, **kw):
        raise OSError("hard links unsupported")

    monkeypatch.setattr(os, "link", no_link)
    # Reader attached with no generation link available -> reads canonical.
    e.readers = 1
    mod.Cache._pin_generation_locked(cache, e)
    assert e.part_is_link is False
    fd = open(e.data_path, "rb")
    assert fd.read() == b"abcdef"

    assert asyncio.run(cache.purge("pkg.bin")) is True
    # Canonical name is gone immediately; generation name holds the bytes.
    assert not (root / "pkg.bin").exists()
    assert (root / "pkg.bin.part-genA").is_file()
    assert not (root / "pkg.bin.meta").exists()
    fd.seek(0)
    assert fd.read() == b"abcdef"  # open reader unaffected
    fd.close()

    asyncio.run(cache.detach(e))
    assert list(root.iterdir()) == []


def test_old_generation_never_deletes_new_canonical_files(cache_mod):
    """The core ownership rule, without HTTP: an old purged generation that
    still has a reader must not remove a newer generation's file/meta."""
    mod, cache, root = cache_mod
    old = _write_complete(cache_mod, "pkg.bin", b"old-bytes", "old")
    old.readers = 1
    old.part_is_link = True
    os.link(root / "pkg.bin", root / "pkg.bin.part-old")

    asyncio.run(cache.purge("pkg.bin"))  # old is draining
    new = _write_complete(cache_mod, "pkg.bin", b"new-bytes", "new")

    asyncio.run(cache.detach(old))  # last old reader exits
    assert (root / "pkg.bin").read_bytes() == b"new-bytes"
    assert (root / "pkg.bin.meta").is_file()
    assert json.loads((root / "pkg.bin.meta").read_text())["gen"] == "new"
    assert not (root / "pkg.bin.part-old").exists()
    assert new.state == "complete"


def test_adopting_disk_rejects_bytes_owned_by_draining_generation(cache_mod):
    """After a purge with a still-draining reader, the canonical file may
    briefly exist; a fresh attach must not adopt it as a cache hit."""
    mod, cache, root = cache_mod
    old = _write_complete(cache_mod, "pkg.bin", b"old-bytes", "old")
    old.readers = 1
    old.part_is_link = True
    os.link(root / "pkg.bin", root / "pkg.bin.part-old")
    # Canonical still present because the generation is draining.
    assert cache._entry_from_disk("pkg.bin") is None

    # Once the old generation is fully gone from the registry/live set, the
    # on-disk package is adoptable again (restart semantics).
    cache.live.discard(old)
    adopted = cache._entry_from_disk("pkg.bin")
    assert adopted is not None and adopted.state == "complete"
    assert adopted.size == 9
    _unlink_all(root)


def test_meta_from_other_generation_is_not_trusted(cache_mod):
    """A meta file whose data_id does not match the package on disk is stale
    (crash between data and meta publish) and must be ignored."""
    mod, cache, root = cache_mod
    e = _write_complete(cache_mod, "pkg.bin", b"data", "genX")
    cache.entries.clear()
    cache.live.clear()
    meta_path = root / "pkg.bin.meta"
    meta = json.loads(meta_path.read_text())
    meta["data_id"] = [999, 999]
    meta_path.write_text(json.dumps(meta))

    adopted = cache._entry_from_disk("pkg.bin")
    assert adopted is not None
    assert adopted.meta.get("sha256") is None  # stale meta discarded
    assert adopted.meta["content_type"] == "application/octet-stream"
    _unlink_all(root)


def test_reserved_sidecar_names_are_rejected():
    for bad in ("a.meta", "a.part-0123abcd", "a.final-0123abcd",
                "a.meta.tmp-0123abcd", "dir.meta/pkg.bin"):
        with pytest.raises(ValueError):
            proxy_app.validate_key(bad)
    assert proxy_app.validate_key("dir/name.bin") == "dir/name.bin"


def _unlink_all(root: Path) -> None:
    for p in root.rglob("*"):
        if p.is_file():
            p.unlink()

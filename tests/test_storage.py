from app.storage import Storage


def test_reconcile_removes_only_unregistered_files(tmp_path):
    data_dir = tmp_path / "data"
    storage = Storage(data_dir)
    storage.write_version(1, b"registered\n")
    storage.write_version(2, b"orphan\n")
    (storage.staging_dir / "leftover.tmp").write_bytes(b"junk")

    moved = storage.reconcile({1})

    assert [p.name for p in moved] == ["v0002.csv"]
    assert storage.version_path(1).exists()
    assert not storage.version_path(2).exists()
    # Data is quarantined, not destroyed.
    assert (data_dir / "orphans" / "v0002.csv").read_bytes() == b"orphan\n"
    assert not list(storage.staging_dir.glob("*.tmp"))


def test_staging_write_is_atomic_and_leaves_internal_tmp_dir_clean(tmp_path):
    storage = Storage(tmp_path / "data")
    storage.write_version(7, b"hello")
    assert storage.version_path(7).read_bytes() == b"hello"
    assert list(storage.staging_dir.iterdir()) == []
    assert not list(storage.versions_dir.glob(".*"))


def test_reconcile_respects_grace_period_for_in_flight_publish(tmp_path):
    storage = Storage(tmp_path / "data")
    storage.write_version(3, b"just written\n")
    assert storage.reconcile(set(), min_age_seconds=3600) == []
    assert storage.version_path(3).exists()

    assert [p.name for p in storage.reconcile(set(), min_age_seconds=0)] == ["v0003.csv"]

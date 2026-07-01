import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from geoseeq.cli._grouping import group_files
from geoseeq.cli.upload.upload_reads import (
    _LINK_TYPE_S3_DEPRECATION_MSG,
    DEFAULT_READ_REPLICATE,
    _bulk_prepare,
    _do_upload,
    _maybe_warn_link_type_s3_deprecated,
    _resolve_read_replicate,
)


class DummyKnex:
    def __init__(self, groups):
        self.groups = groups
        self.calls = []

    def post(self, endpoint, json):
        self.calls.append((endpoint, json))
        if endpoint == "bulk_upload/group_files":
            return self.groups
        raise AssertionError(f"Unexpected endpoint {endpoint}")


def test_group_files_applies_name_map(tmp_path: Path):
    name_map_file = tmp_path / "name_map.csv"
    name_map_file.write_text("current,new\nold_name,new_name\n")

    groups = [
        {"sample_name": "old_name", "fields": {"R1": "old_name_R1.fastq"}},
    ]
    knex = DummyKnex(groups)

    filepaths = {"old_name_R1.fastq": "/tmp/old_name_R1.fastq"}

    updated_groups = group_files(
        knex,
        filepaths,
        "short_read::single_end",
        regex=r"(?P<sample_name>.+)",
        yes=True,
        name_map=(name_map_file, "current", "new"),
    )

    assert updated_groups[0]["sample_name"] == "new_name"
    # Ensure the grouping endpoint was called using the provided paths.
    assert knex.calls[0][0] == "bulk_upload/group_files"
    assert filepaths.keys() == set(knex.calls[0][1]["filenames"])


def test_group_files_without_name_map():
    """group_files() with name_map=None (the new default) leaves sample names unchanged."""
    groups = [
        {"sample_name": "sample_A", "fields": {"R1": "sample_A_R1.fastq"}},
    ]
    knex = DummyKnex(groups)
    filepaths = {"sample_A_R1.fastq": "/tmp/sample_A_R1.fastq"}

    result = group_files(
        knex,
        filepaths,
        "short_read::single_end",
        regex=r"(?P<sample_name>.+)",
        yes=True,
        # name_map omitted — exercises the None default path
    )

    assert result[0]["sample_name"] == "sample_A"
    assert knex.calls[0][0] == "bulk_upload/group_files"
    assert filepaths.keys() == set(knex.calls[0][1]["filenames"])


def test_group_files_confirm_aborts_when_not_yes():
    """group_files() with yes=False aborts (via click.Abort) when the user declines the prompt.

    CliRunner surfaces the abort as a non-zero exit code.
    """
    from click.testing import CliRunner
    import click

    groups = [
        {"sample_name": "sample_B", "fields": {"R1": "sample_B_R1.fastq"}},
    ]
    knex = DummyKnex(groups)
    filepaths = {"sample_B_R1.fastq": "/tmp/sample_B_R1.fastq"}

    @click.command()
    def _run():
        group_files(
            knex,
            filepaths,
            "short_read::single_end",
            regex=r"(?P<sample_name>.+)",
            yes=False,
        )

    runner = CliRunner()
    # Simulate the user typing "n" at the confirmation prompt.
    result = runner.invoke(_run, input="n\n")
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# LR-05: deprecation of `upload reads --link-type s3` for local-file-list
# ---------------------------------------------------------------------------


def _local_filepaths():
    """Return a filepaths-shape dict with local paths (no s3:// values)."""
    return {
        "sample_A_R1.fastq.gz": "/data/reads/sample_A_R1.fastq.gz",
        "sample_A_R2.fastq.gz": "/data/reads/sample_A_R2.fastq.gz",
    }


def test_link_type_s3_with_local_paths_warns(capsys):
    """`--link-type s3` over local paths must fire DeprecationWarning + stderr echo."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("s3", _local_filepaths())

    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert _LINK_TYPE_S3_DEPRECATION_MSG in str(caught[0].message)

    # User-visible stderr echo (so warning filters can't silence it).
    err = capsys.readouterr().err
    assert "DeprecationWarning" in err
    assert "geoseeq link reads" in err
    assert "geoseeq s3 register" in err


def test_link_type_upload_does_not_warn(capsys):
    """Default byte-upload mode must NOT emit the deprecation warning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("upload", _local_filepaths())

    assert caught == []
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("other_link_type", ["ftp", "sra", "azure", "http"])
def test_link_type_other_does_not_warn(other_link_type, capsys):
    """Other --link-type values (ftp/sra/azure/http) are out of scope."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated(other_link_type, _local_filepaths())

    assert caught == []
    assert capsys.readouterr().err == ""


def test_link_type_s3_with_s3_uri_paths_does_not_warn(capsys):
    """If the file list values are themselves s3:// URIs, don't warn (out of scope)."""
    s3_paths = {
        "sample_A_R1.fastq.gz": "s3://my-bucket/path/sample_A_R1.fastq.gz",
        "sample_A_R2.fastq.gz": "s3://my-bucket/path/sample_A_R2.fastq.gz",
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _maybe_warn_link_type_s3_deprecated("s3", s3_paths)

    assert caught == []
    assert capsys.readouterr().err == ""


def test_deprecation_warning_stacklevel_attributes_to_caller():
    """stacklevel=2 must attribute the warning to the direct caller, not the helper.

    This guards against regression where changing stacklevel would make the
    warning appear to originate from inside ``_maybe_warn_link_type_s3_deprecated``
    (upload_reads.py) rather than the call-site in the enclosing command function.
    We simulate the one-level-of-indirection present in the real usage
    (cli_upload_reads_wizard -> helper) by wrapping the call in a local function
    and asserting the recorded filename points at THIS test file, not at
    upload_reads.py.
    """
    import os

    def _simulate_cli_call_site():
        """Represents the call inside cli_upload_reads_wizard."""
        _maybe_warn_link_type_s3_deprecated("s3", _local_filepaths())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _simulate_cli_call_site()

    assert len(caught) == 1
    w = caught[0]
    # stacklevel=2 means: skip the helper frame, attribute to *its* caller.
    # Here that caller is _simulate_cli_call_site defined in this file.
    assert os.path.basename(w.filename) == "test_upload_reads_unit.py", (
        f"Warning attributed to {w.filename!r}; expected this test file. "
        "stacklevel inside _maybe_warn_link_type_s3_deprecated is wrong."
    )


# ---------------------------------------------------------------------------
# URB-04: _bulk_prepare unit tests
# ---------------------------------------------------------------------------


class _FakeFile:
    """In-memory stand-in for a ResultFile/SampleResultFile.

    Tracks whether ``.idem()`` was invoked so tests can assert on the
    already-exists fallback path.
    """

    def __init__(self, folder, name):
        self.parent = folder
        self.name = name
        self.uuid = None
        self.idem_called = False

    def idem(self):
        self.idem_called = True
        # Mimic the server populating the UUID on existing rows.
        self.uuid = f"file-uuid-{self.name}"
        return self


class _FakeFolder:
    """In-memory stand-in for a SampleResultFolder."""

    def __init__(self, sample, module_name, replicate=None, updated_at=None):
        self.parent = sample
        self.module_name = module_name
        self.replicate = replicate
        self.updated_at = updated_at
        # SampleResultFolder name attribute is the module_name for our purposes.
        self.name = module_name
        self.uuid = None
        self.idem_called = False
        self._files = {}

    def read_file(self, field_name):
        if field_name not in self._files:
            self._files[field_name] = _FakeFile(self, field_name)
        return self._files[field_name]

    def result_file(self, field_name):
        """Stand-in for ``SampleResultFolder.result_file``.

        ``_bulk_prepare`` calls this directly with the fully-prefixed
        canonical name (e.g. ``short_read::single_end::read_1::lane_1``),
        bypassing ``read_file`` so the call site doesn't rely on the
        defensive prefix-normalization in ``SampleBioInfoFolder.read_file``.
        """
        if field_name not in self._files:
            self._files[field_name] = _FakeFile(self, field_name)
        return self._files[field_name]

    def idem(self):
        self.idem_called = True
        self.uuid = f"folder-uuid-{self.parent.name}"
        return self


class _FakeSample:
    """In-memory stand-in for a Sample."""

    def __init__(self, lib, name, existing_folders=None):
        self.parent = lib
        self.name = name
        self.uuid = None
        self.idem_called = False
        self._folders = {}
        # Folders the server already has for this sample; drives the
        # replicate-resolution lookup in _bulk_prepare.
        self._existing_folders = list(existing_folders or [])

    def result_folder(self, module_name, replicate=None):
        if module_name not in self._folders:
            folder = _FakeFolder(self, module_name)
            folder.replicate = replicate
            self._folders[module_name] = folder
        return self._folders[module_name]

    def get_result_folders(self):
        return self._existing_folders

    def idem(self):
        self.idem_called = True
        self.uuid = f"sample-uuid-{self.name}"
        return self


class _FakeLib:
    """In-memory stand-in for a Project."""

    def __init__(self):
        self.knex = MagicMock(name="knex")
        self._samples = {}

    def sample(self, name):
        # The helper builds the in-memory dict keyed by name; return a
        # fresh object each call so identity matches what the helper
        # stores (it constructs once per unique name).
        if name not in self._samples:
            self._samples[name] = _FakeSample(self, name)
        return self._samples[name]


def _patch_bulk(monkeypatch, samples_cb, folders_cb, files_cb):
    """Patch the three bulk_create_* functions used by _bulk_prepare.

    Each callback receives the list of in-memory objects the helper hands
    to the bulk creator and returns the list of "created" rows (subset).
    Also pre-populates UUIDs on the returned objects so downstream phases
    can rewire by parent uuid.
    """
    calls = {"samples": 0, "folders": 0, "files": 0}

    def fake_samples(knex, samples):
        calls["samples"] += 1
        return samples_cb(samples)

    def fake_folders(knex, folders):
        calls["folders"] += 1
        return folders_cb(folders)

    def fake_files(knex, files):
        calls["files"] += 1
        return files_cb(files)

    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_samples", fake_samples
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_folders",
        fake_folders,
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_files",
        fake_files,
    )
    return calls


def test_bulk_prepare_mixed_created_and_existing_need_file_uuids_true(monkeypatch):
    """Helper returns a key for every group: created rows + .idem() fallbacks.

    Server returns sample_A as created and sample_B as already-existing;
    folder for A is created, folder for B is not; file R1 of A is created,
    the other files are not. With need_file_uuids=True every file object
    in the mapping must end up with a UUID — created rows from the bulk
    POST or .idem() on the in-memory object.
    """
    lib = _FakeLib()
    groups = [
        {"sample_name": "A", "fields": {"R1": "A_R1.fastq", "R2": "A_R2.fastq"}},
        {"sample_name": "B", "fields": {"R1": "B_R1.fastq"}},
    ]

    def samples_cb(in_memory_samples):
        # Server says A is newly created; B already existed.
        for s in in_memory_samples:
            if s.name == "A":
                s.uuid = "sample-uuid-A"
                return [s]
        return []

    def folders_cb(in_memory_folders):
        # Server says folder under A is newly created.
        for f in in_memory_folders:
            if f.parent.name == "A":
                f.uuid = "folder-uuid-A"
                return [f]
        return []

    def files_cb(in_memory_files):
        # Server says only the A/R1 file is newly created. The file's
        # name is now built by ``_bulk_prepare`` from
        # ``seq_length`` + ``field_name`` — in this test the bare field
        # names (``R1``/``R2``) give ``short_read::R1``/``short_read::R2``.
        # Real ``field_name`` values from ``bulk_upload/group_files`` are
        # already seq-type-prefixed (``paired_end::read_1::lane_1``); the
        # canonical-name regression test below exercises that shape.
        for f in in_memory_files:
            if f.parent.parent.name == "A" and f.name == "short_read::R1":
                f.uuid = "file-uuid-A-R1"
                return [f]
        return []

    calls = _patch_bulk(monkeypatch, samples_cb, folders_cb, files_cb)

    mapping = _bulk_prepare(
        lib.knex, lib, groups, "short_read::paired_end", need_file_uuids=True
    )

    # Every (sample_name, field_name) is present in the mapping.
    assert set(mapping.keys()) == {
        ("A", "R1"), ("A", "R2"), ("B", "R1"),
    }
    # Each phase fired exactly once.
    assert calls == {"samples": 1, "folders": 1, "files": 1}

    # B was missing from the samples response, so .idem() was used.
    assert lib._samples["B"].idem_called is True
    assert lib._samples["A"].idem_called is False

    # Folder under B was missing -> .idem() was used.
    b_folder = lib._samples["B"]._folders["short_read::paired_end"]
    a_folder = lib._samples["A"]._folders["short_read::paired_end"]
    assert b_folder.idem_called is True
    assert a_folder.idem_called is False

    # Files: A/R1 was created -> no idem; A/R2 + B/R1 missing -> idem.
    assert mapping[("A", "R1")].idem_called is False
    assert mapping[("A", "R2")].idem_called is True
    assert mapping[("B", "R1")].idem_called is True


def test_bulk_prepare_skips_file_fallback_when_need_file_uuids_false(monkeypatch):
    """Atomic-upload path: missing files are NOT .idem()'d.

    The atomic upload route never reads ``file.uuid`` — it uses the parent
    folder UUID — so skipping the per-file fallback is intentional and
    keeps the fast path round-trip-free.
    """
    lib = _FakeLib()
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]

    calls = _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [setattr(s, "uuid", f"uuid-{s.name}") or s for s in ss],
        folders_cb=lambda fs: [setattr(f, "uuid", f"folder-uuid-{f.parent.name}") or f for f in fs],
        files_cb=lambda fs: [],  # nothing returned
    )

    mapping = _bulk_prepare(
        lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False
    )

    assert set(mapping.keys()) == {("A", "R1")}
    # Bulk POST still fired (manifest side-effect), but no per-file fallback.
    assert calls == {"samples": 1, "folders": 1, "files": 1}
    assert mapping[("A", "R1")].idem_called is False


def test_bulk_prepare_raises_phase_named_error_on_samples_failure(monkeypatch):
    """A samples-phase failure aborts with a phase-named RuntimeError.

    Subsequent phases (folders, files) must not be attempted — a partial
    bulk-then-fallback would mask the real server error.
    """
    lib = _FakeLib()
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]

    def samples_cb(_samples):
        raise ValueError("backend exploded")

    folders_called = {"n": 0}
    files_called = {"n": 0}

    def folders_cb(_folders):
        folders_called["n"] += 1
        return []

    def files_cb(_files):
        files_called["n"] += 1
        return []

    _patch_bulk(monkeypatch, samples_cb, folders_cb, files_cb)

    with pytest.raises(RuntimeError, match="samples"):
        _bulk_prepare(
            lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False
        )

    assert folders_called["n"] == 0
    assert files_called["n"] == 0


def test_do_upload_does_no_bulk_posts_when_user_declines(monkeypatch):
    """confirm-before-write: if the user declines at the group_files
    prompt, _do_upload is never entered, so zero bulk POSTs fire.

    This guards the invariant that the click.confirm in group_files
    runs before any server writes.
    """
    import click

    # Patch the bulk creators to record any (unexpected) calls.
    posts = []
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_samples",
        lambda *a, **kw: posts.append("samples") or [],
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_folders",
        lambda *a, **kw: posts.append("folders") or [],
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_files",
        lambda *a, **kw: posts.append("files") or [],
    )

    # Patch click.confirm with a MagicMock so we can both (a) trigger the
    # decline by raising click.Abort and (b) prove that the abort actually
    # came from the confirm gate (not from some earlier exception in the
    # fake knex / group_files plumbing).
    mock_confirm = MagicMock(side_effect=click.Abort())
    monkeypatch.setattr("geoseeq.cli._grouping.click.confirm", mock_confirm)

    # group_files calls knex.post('bulk_upload/group_files') then
    # click.confirm(); declining raises click.Abort.
    groups_response = [
        {"sample_name": "A", "fields": {"R1": "A_R1.fastq"}},
    ]
    knex = DummyKnex(groups_response)
    filepaths = {"A_R1.fastq": "/tmp/A_R1.fastq"}

    with pytest.raises(click.Abort):
        groups = group_files(
            knex, filepaths,
            "short_read::single_end",
            regex=r"(?P<sample_name>.+)",
            yes=False,
        )
        # If group_files ever returns (it shouldn't when user declines),
        # _do_upload would be called — but the abort short-circuits.
        _do_upload(
            groups,
            "short_read::single_end",
            "upload",
            lib=_FakeLib(),
            filepaths=filepaths,
            overwrite=False,
            no_new_versions=False,
            cores=1,
            state=MagicMock(),
        )

    # Sanity: the abort must have come from the confirm gate, not from an
    # earlier failure in the fake knex setup. Without this assertion the
    # ``posts == []`` check could pass vacuously.
    assert mock_confirm.call_count >= 1, (
        "click.confirm was never reached — the abort came from somewhere "
        "upstream, so this test is not actually exercising the confirm gate."
    )
    assert posts == [], f"Bulk POSTs fired before user confirmed: {posts}"


def test_confirm_gates_all_bulk_posts(monkeypatch):
    """The click.confirm gate must run BEFORE any bulk_* POST is issued.

    Stronger version of ``test_do_upload_does_no_bulk_posts_when_user_declines``:
    explicitly constructs a scenario where ``_bulk_prepare`` *would* be
    invoked if anyone reordered the call sites, and asserts that no bulk
    POST is hit.

    Patches ``click.confirm`` to raise ``click.Abort`` directly (no CliRunner
    stdin needed), and drives the same ``group_files`` -> ``_do_upload`` chain
    that ``cli_upload_reads_wizard`` uses. With ``_do_upload`` and
    ``_bulk_prepare`` fully wired up to the bulk creator stubs, the test will
    fail if anyone moves the confirm into ``_do_upload`` *after*
    ``_bulk_prepare`` — because the bulk POSTs would fire before the relocated
    confirm aborts.
    """
    import click

    posts = []
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_samples",
        lambda *a, **kw: posts.append("samples") or [],
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_folders",
        lambda *a, **kw: posts.append("folders") or [],
    )
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.bulk_create_sample_result_files",
        lambda *a, **kw: posts.append("files") or [],
    )

    # Patch click.confirm in both modules where a future regression could
    # add it (group_files today, _do_upload under the mutation). We use a
    # MagicMock so the test can prove the abort actually came from the
    # confirm gate — without this, the ``posts == []`` assertion below
    # could pass vacuously if some upstream code in the fake plumbing
    # raised before click.confirm was ever reached.
    mock_confirm = MagicMock(side_effect=click.Abort())
    monkeypatch.setattr("geoseeq.cli._grouping.click.confirm", mock_confirm)
    monkeypatch.setattr(
        "geoseeq.cli.upload.upload_reads.click.confirm",
        mock_confirm,
        raising=False,
    )

    groups_response = [
        {"sample_name": "A", "fields": {"R1": "A_R1.fastq", "R2": "A_R2.fastq"}},
    ]
    knex = DummyKnex(groups_response)
    filepaths = {
        "A_R1.fastq": "/tmp/A_R1.fastq",
        "A_R2.fastq": "/tmp/A_R2.fastq",
    }
    lib = _FakeLib()

    with pytest.raises(click.Abort):
        groups = group_files(
            knex,
            filepaths,
            "short_read::paired_end",
            regex=r"(?P<sample_name>.+)",
            yes=False,
        )
        # If a future change removes the confirm from group_files and puts
        # it inside _do_upload after _bulk_prepare, this call will execute
        # and _bulk_prepare will fire the bulk POSTs *before* the abort.
        _do_upload(
            groups,
            "short_read::paired_end",
            "upload",
            lib=lib,
            filepaths=filepaths,
            overwrite=False,
            no_new_versions=False,
            cores=1,
            state=MagicMock(),
        )

    # Sanity: the abort must have come from the confirm gate, not from an
    # earlier failure in the fake knex / _bulk_prepare plumbing.
    assert mock_confirm.call_count >= 1, (
        "click.confirm was never reached — the abort came from somewhere "
        "upstream, so this test is not actually exercising the confirm gate."
    )
    assert posts == [], (
        f"bulk POSTs fired before the confirm gate aborted: {posts}. "
        "The click.confirm in group_files must run before any bulk_* POST."
    )


def test_bulk_prepare_all_existing_returns_full_mapping(monkeypatch):
    """All-existing path: every bulk POST returns [] and ``.idem()`` rehydrates.

    Covers the end-of-spectrum case where every sample, folder, and file
    already exists on the server. The three ``bulk_*`` POSTs return empty
    lists (server silently skipped all duplicates), so the helper must fall
    back to ``.idem()`` on every in-memory object and still return a fully
    populated ``(sample_name, field_name) -> file`` mapping.

    If anyone deletes the fallback ``.idem()`` loop in any phase, this test
    fails because the mapping would have no UUIDs and ``idem_called`` would
    stay ``False``.
    """
    lib = _FakeLib()
    groups = [
        {"sample_name": "A", "fields": {"R1": "A_R1.fastq", "R2": "A_R2.fastq"}},
        {"sample_name": "B", "fields": {"R1": "B_R1.fastq"}},
    ]

    # Every bulk endpoint returns [] -> "all rows already existed".
    calls = _patch_bulk(
        monkeypatch,
        samples_cb=lambda _samples: [],
        folders_cb=lambda _folders: [],
        files_cb=lambda _files: [],
    )

    mapping = _bulk_prepare(
        lib.knex, lib, groups, "short_read::paired_end", need_file_uuids=True
    )

    # Mapping is fully populated for every (sample, field) pair.
    assert set(mapping.keys()) == {("A", "R1"), ("A", "R2"), ("B", "R1")}

    # All three bulk phases fired exactly once.
    assert calls == {"samples": 1, "folders": 1, "files": 1}

    # Every sample fell back to .idem() (none were returned by bulk POST).
    for sample_name in ("A", "B"):
        assert lib._samples[sample_name].idem_called is True, (
            f"sample {sample_name!r} was not .idem()'d despite missing from bulk response"
        )
        assert lib._samples[sample_name].uuid is not None

    # Every folder fell back to .idem().
    for sample_name in ("A", "B"):
        folder = lib._samples[sample_name]._folders["short_read::paired_end"]
        assert folder.idem_called is True, (
            f"folder under {sample_name!r} was not .idem()'d"
        )
        assert folder.uuid is not None

    # Every file fell back to .idem() and has a uuid.
    for key, in_memory_file in mapping.items():
        assert in_memory_file.idem_called is True, (
            f"file {key!r} was not .idem()'d despite missing from bulk response"
        )
        assert in_memory_file.uuid is not None


def test_bulk_prepare_deduplicates_repeated_sample_names(monkeypatch):
    """Groups with the same sample_name are deduplicated before the bulk POST.

    When two groups share a sample_name (e.g. different lanes of the same
    sample), _bulk_prepare must hit the samples bulk-create endpoint exactly
    once per unique name (not once per group), and must still return a key
    for every (sample_name, field_name) pair across all groups.

    Covers the ``continue`` branch on line 96 of upload_reads.py.
    """
    lib = _FakeLib()
    groups = [
        {"sample_name": "A", "fields": {"R1": "A_L1_R1.fastq"}},
        {"sample_name": "A", "fields": {"R2": "A_L1_R2.fastq"}},  # same sample, second group
    ]

    seen_sample_names = []

    def samples_cb(in_memory_samples):
        seen_sample_names.extend(s.name for s in in_memory_samples)
        for s in in_memory_samples:
            s.uuid = f"sample-uuid-{s.name}"
        return in_memory_samples

    calls = _patch_bulk(
        monkeypatch,
        samples_cb=samples_cb,
        folders_cb=lambda fs: [setattr(f, "uuid", f"folder-uuid-{f.parent.name}") or f for f in fs],
        files_cb=lambda files: [],  # all fall back to .idem()
    )

    mapping = _bulk_prepare(
        lib.knex, lib, groups, "short_read::paired_end", need_file_uuids=False
    )

    # Only one unique sample name should have been sent to the bulk POST.
    assert seen_sample_names == ["A"], (
        f"Expected exactly one 'A' sent to bulk_create_samples; got {seen_sample_names!r}"
    )
    # Both (sample, field) keys are present in the mapping despite dedup.
    assert set(mapping.keys()) == {("A", "R1"), ("A", "R2")}
    assert calls["samples"] == 1


def test_bulk_prepare_raises_phase_named_error_on_folders_failure(monkeypatch):
    """A folders-phase failure aborts with a 'folders'-named RuntimeError.

    The files phase must not be attempted after a folder failure.
    Covers lines 119-120 of upload_reads.py.
    """
    lib = _FakeLib()
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]

    files_called = {"n": 0}

    def folders_cb(_folders):
        raise ConnectionError("folders backend down")

    def files_cb(_files):
        files_called["n"] += 1
        return []

    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [setattr(s, "uuid", f"uuid-{s.name}") or s for s in ss],
        folders_cb=folders_cb,
        files_cb=files_cb,
    )

    with pytest.raises(RuntimeError, match="folders"):
        _bulk_prepare(lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False)

    assert files_called["n"] == 0


def test_bulk_prepare_raises_phase_named_error_on_files_failure(monkeypatch):
    """A files-phase failure aborts with a 'files'-named RuntimeError.

    Covers lines 143-144 of upload_reads.py.
    """
    lib = _FakeLib()
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]

    def files_cb(_files):
        raise IOError("files backend down")

    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [setattr(s, "uuid", f"uuid-{s.name}") or s for s in ss],
        folders_cb=lambda fs: [setattr(f, "uuid", f"folder-uuid-{f.parent.name}") or f for f in fs],
        files_cb=files_cb,
    )

    with pytest.raises(RuntimeError, match="files"):
        _bulk_prepare(lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False)


def test_bulk_prepare_builds_canonical_file_name_without_double_prefix(monkeypatch):
    """Regression: the file name passed to the bulk creator is fully prefixed exactly once.

    The server's ``bulk_upload/group_files`` endpoint returns ``field_name``
    values already prefixed with the seq-type (e.g.
    ``single_end::read_1::lane_1``). ``_bulk_prepare`` must build the
    canonical file name by prepending only the top-level ``seq_length``
    (``short_read``), yielding ``short_read::single_end::read_1::lane_1``
    — not the doubled ``short_read::single_end::single_end::read_1::lane_1``
    that the original ``folder.read_file(field_name)`` call produced when
    paired with the un-normalized ``read_file`` contract.

    This locks the call-site fix and matches the canonical name rendered
    by the upload preview in ``_grouping.group_files``.
    """
    lib = _FakeLib()
    groups = [
        {
            "sample_name": "S1",
            "fields": {"single_end::read_1::lane_1": "S1_L1_R1.fastq.gz"},
        },
    ]

    seen_file_names = []

    def files_cb(in_memory_files):
        seen_file_names.extend(f.name for f in in_memory_files)
        return []

    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [setattr(s, "uuid", f"uuid-{s.name}") or s for s in ss],
        folders_cb=lambda fs: [setattr(f, "uuid", f"folder-uuid-{f.parent.name}") or f for f in fs],
        files_cb=files_cb,
    )

    _bulk_prepare(
        lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False
    )

    assert seen_file_names == ["short_read::single_end::read_1::lane_1"], (
        f"Expected single sub-type prefix; got {seen_file_names!r}. "
        "Double prefix indicates _bulk_prepare is re-applying the seq-type "
        "that the server already added to field_name."
    )
    # Explicit guard against the double-prefix bug shape.
    assert "single_end::single_end" not in seen_file_names[0], (
        f"Doubled sub-type prefix in {seen_file_names[0]!r}"
    )


# ---------------------------------------------------------------------------
# TKT-101: replicate resolution -- make "replace / new version" the default
# ---------------------------------------------------------------------------


def test_resolve_read_replicate_no_existing_returns_default():
    """A sample with no reads folder for the module gets the canonical '1'."""
    lib = _FakeLib()
    sample = _FakeSample(lib, "S")  # no existing folders
    assert (
        _resolve_read_replicate(sample, "short_read::single_end")
        == DEFAULT_READ_REPLICATE
    )


def test_resolve_read_replicate_adopts_single_existing():
    """One existing folder (even under a random replicate) is reused in place."""
    lib = _FakeLib()
    module = "short_read::single_end"
    existing = _FakeFolder(None, module, replicate="a1b2c3d4e5f6")
    sample = _FakeSample(lib, "S", existing_folders=[existing])
    assert _resolve_read_replicate(sample, module) == "a1b2c3d4e5f6"


def test_resolve_read_replicate_ignores_other_modules():
    """Existing folders for a different module don't count."""
    lib = _FakeLib()
    other = _FakeFolder(None, "long_read::nanopore", replicate="zzz")
    sample = _FakeSample(lib, "S", existing_folders=[other])
    assert (
        _resolve_read_replicate(sample, "short_read::single_end")
        == DEFAULT_READ_REPLICATE
    )


def test_resolve_read_replicate_multiple_picks_most_recent_and_warns(capsys):
    """Ambiguous legacy state: use most-recently-updated folder, warn the user."""
    lib = _FakeLib()
    module = "short_read::single_end"
    older = _FakeFolder(None, module, replicate="old", updated_at="2020-01-01")
    newer = _FakeFolder(None, module, replicate="new", updated_at="2024-06-01")
    sample = _FakeSample(lib, "S", existing_folders=[older, newer])
    assert _resolve_read_replicate(sample, module) == "new"
    err = capsys.readouterr().err
    assert "2 'short_read::single_end' read folders" in err
    assert "--replicate" in err


def test_bulk_prepare_new_sample_uses_default_replicate(monkeypatch):
    """Brand-new samples upload into DEFAULT_READ_REPLICATE (no lookup)."""
    lib = _FakeLib()
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]
    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [setattr(s, "uuid", f"uuid-{s.name}") or s for s in ss],
        folders_cb=lambda fs: [setattr(f, "uuid", f"folder-uuid-{f.parent.name}") or f for f in fs],
        files_cb=lambda fs: fs,
    )
    _bulk_prepare(lib.knex, lib, groups, "short_read::single_end", need_file_uuids=False)
    folder = lib._samples["A"]._folders["short_read::single_end"]
    assert folder.replicate == DEFAULT_READ_REPLICATE


def test_bulk_prepare_existing_sample_adopts_existing_replicate(monkeypatch):
    """A pre-existing sample reuses its existing reads folder's replicate."""
    lib = _FakeLib()
    module = "short_read::single_end"
    # Pre-seed the sample as already existing with a random-replicate folder.
    existing = _FakeFolder(None, module, replicate="rand123")
    sample = _FakeSample(lib, "A", existing_folders=[existing])
    lib._samples["A"] = sample
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]
    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [],  # server reports none created -> pre-existing
        folders_cb=lambda fs: [],
        files_cb=lambda fs: [],
    )
    _bulk_prepare(lib.knex, lib, groups, module, need_file_uuids=False)
    assert sample._folders[module].replicate == "rand123"


def test_bulk_prepare_explicit_replicate_overrides(monkeypatch):
    """An explicit replicate is used verbatim, skipping resolution."""
    lib = _FakeLib()
    module = "short_read::single_end"
    existing = _FakeFolder(None, module, replicate="rand123")
    sample = _FakeSample(lib, "A", existing_folders=[existing])
    lib._samples["A"] = sample
    groups = [{"sample_name": "A", "fields": {"R1": "A_R1.fastq"}}]
    _patch_bulk(
        monkeypatch,
        samples_cb=lambda ss: [],
        folders_cb=lambda fs: [],
        files_cb=lambda fs: [],
    )
    _bulk_prepare(lib.knex, lib, groups, module, need_file_uuids=False, replicate="lane2")
    assert sample._folders[module].replicate == "lane2"

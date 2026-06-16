# pylint: disable=line-too-long
import logging
import warnings
import click
import requests
from os.path import basename
from multiprocessing import current_process

from geoseeq.cli.constants import *
from geoseeq.cli._grouping import get_regex, group_files
from geoseeq.cli.shared_params import (
    handle_project_id,
    private_option,
    link_option,
    module_option,
    project_id_arg,
    overwrite_option,
    yes_option,
    use_common_state,
    no_new_versions_option
)
from geoseeq.upload_download_manager import GeoSeeqUploadManager

from geoseeq.bulk_creators import (
    bulk_create_samples,
    bulk_create_sample_result_folders,
    bulk_create_sample_result_files,
)
from geoseeq.constants import FASTQ_MODULE_NAMES
from geoseeq.cli.progress_bar import PBarManager

logger = logging.getLogger('geoseeq_api')


def _make_in_process_logger(log_level):
    logger = logging.getLogger('geoseeq_api')
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s :: ' + current_process().name + ' :: %(message)s'))
    logger.addHandler(handler)
    return logger


def _upload_one_file(args):
    result_file, filepath, session, progress_tracker, link_type, overwrite, log_level = args
    _make_in_process_logger(log_level)
    if link_type == 'upload':
        # TODO: check checksums to see if the file is the same
        result_file.upload_file(filepath, session=session, overwrite=overwrite,progress_tracker=progress_tracker, threads=4)
    else:
        result_file.link_file(link_type, filepath)


def _bulk_prepare(knex, lib, groups, module_name, need_file_uuids):
    """Pre-create samples, result folders, and result files for ``groups`` in three bulk POSTs.

    Replaces the previous O(N) per-group ``Sample().idem()`` /
    ``ResultFolder().idem()`` / ``read_file()`` chain with three round-trips
    total (one per phase), then falls back to per-object ``.idem()`` for any
    names the bulk POST did not return (the "already-exists" case — the bulk
    endpoints silently skip duplicates and only return *newly created* rows).

    Args:
        knex: GeoSeeq client used for the bulk POSTs and fallback GETs.
        lib: Project the samples belong to. ``lib.sample(name)`` is the
            in-memory constructor used to build sample objects.
        groups: List of grouping dicts as produced by ``group_files`` —
            each entry has a ``sample_name`` and a ``fields`` mapping
            ``{field_name: path}``.
        module_name: Module name for the read folder (e.g.
            ``short_read::paired_end``).
        need_file_uuids: If ``True``, fall back to ``.idem()`` for any file
            whose UUID wasn't returned by the bulk POST. The non-atomic
            upload path (``upload_advanced._find_target_urls``) needs file
            UUIDs to call ``/ar_fields/{uuid}/create_upload``. The atomic
            upload path (``_do_upload``) does not — it uses the *folder*
            UUID via ``/ars/{folder_uuid}/create_atomic_upload`` (see
            ``geoseeq/result/file_upload.py:_create_multipart_upload``), so
            the bulk POST is fire-and-forget for the manifest side-effect.

    Returns:
        Dict mapping ``(sample_name, field_name)`` to the corresponding
        result-file object, in the same iteration order as ``groups``.

    Raises:
        RuntimeError: If any bulk POST fails — the message names the phase
            (samples / folders / files) and the original exception is
            chained. No partial fallback is attempted; the caller aborts.
    """
    # The server's ``bulk_upload/group_files`` endpoint already prefixes
    # field names with the seq-type (e.g. ``single_end::read_1::lane_1``).
    # Build the canonical fully-prefixed file name from the top-level
    # ``seq_length`` (e.g. ``short_read``) here so we match the preview
    # rendered by ``_grouping.group_files`` exactly and avoid relying on
    # ``read_file``'s defensive normalization at the call site.
    seq_length = module_name.split('::')[0]

    # Phase 1: samples.
    unique_names = []
    samples_by_name = {}
    for group in groups:
        name = group['sample_name']
        if name in samples_by_name:
            continue
        samples_by_name[name] = lib.sample(name)
        unique_names.append(name)
    try:
        created_samples = bulk_create_samples(knex, list(samples_by_name.values()))
    except Exception as exc:
        raise RuntimeError(f"Bulk create failed during 'samples' phase: {exc}") from exc
    created_sample_names = {s.name for s in created_samples}
    for created in created_samples:
        samples_by_name[created.name] = created
    for name in unique_names:
        if name not in created_sample_names:
            samples_by_name[name].idem()  # populates the in-memory object

    # Phase 2: result folders (one per sample).
    folders_by_sample_name = {
        name: samples_by_name[name].result_folder(module_name)
        for name in unique_names
    }
    try:
        created_folders = bulk_create_sample_result_folders(
            knex, list(folders_by_sample_name.values())
        )
    except Exception as exc:
        raise RuntimeError(f"Bulk create failed during 'folders' phase: {exc}") from exc
    # Bulk folder responses are keyed by their parent sample's UUID; build a
    # uuid -> sample_name lookup so we can rewire by sample name.
    sample_name_by_uuid = {samples_by_name[name].uuid: name for name in unique_names}
    created_folder_sample_names = set()
    for created in created_folders:
        sample_name = sample_name_by_uuid.get(created.parent.uuid)
        if sample_name is not None:
            folders_by_sample_name[sample_name] = created
            created_folder_sample_names.add(sample_name)
    for name in unique_names:
        if name not in created_folder_sample_names:
            folders_by_sample_name[name].idem()

    # Phase 3: result files. Build the (sample_name, field_name) -> file map
    # in the same iteration order as `groups`.
    files_by_key = {}
    for group in groups:
        folder = folders_by_sample_name[group['sample_name']]
        for field_name in group['fields']:
            full_name = f'{seq_length}::{field_name}'
            files_by_key[(group['sample_name'], field_name)] = folder.result_file(full_name)
    try:
        created_files = bulk_create_sample_result_files(knex, list(files_by_key.values()))
    except Exception as exc:
        raise RuntimeError(f"Bulk create failed during 'files' phase: {exc}") from exc
    if need_file_uuids:
        # The bulk endpoint returns rows keyed by canonical file name +
        # parent folder UUID, not by the bare field_name we hand in. Index
        # the in-memory files by (folder uuid, canonical file name) so we
        # can rewire any matches; .idem() the rest.
        files_by_match = {
            (in_memory.parent.uuid, in_memory.name): key
            for key, in_memory in files_by_key.items()
        }
        created_keys = set()
        for created in created_files:
            key = files_by_match.get((created.parent.uuid, created.name))
            if key is not None:
                files_by_key[key] = created
                created_keys.add(key)
        for key, in_memory_file in list(files_by_key.items()):
            if key not in created_keys:
                in_memory_file.idem()
    # When need_file_uuids is False (atomic upload path), the in-memory
    # `read_file()` objects are sufficient — the upload path only reads the
    # parent folder's UUID, never the file's. The bulk POST above is still
    # made for the server-side manifest side effect.

    return files_by_key


def _do_upload(groups, module_name, link_type, lib, filepaths, overwrite, no_new_versions, cores, state):

    with requests.Session() as session:
        upload_manager = GeoSeeqUploadManager(
            n_parallel_uploads=cores,
            session=session,
            link_type=link_type,
            log_level=state.log_level,
            overwrite=overwrite,
            progress_tracker_factory=PBarManager().get_new_bar,
            use_cache=state.use_cache,
            no_new_versions=no_new_versions,
            use_atomic_upload=True,
        )
        files_by_key = _bulk_prepare(
            lib.knex, lib, groups, module_name, need_file_uuids=False
        )
        for group in groups:
            for field_name, path in group['fields'].items():
                result_file = files_by_key[(group['sample_name'], field_name)]
                upload_manager.add_result_file(result_file, filepaths[path])
        upload_manager.upload_files()



_LINK_TYPE_S3_DEPRECATION_MSG = (
    "`geoseeq upload reads --link-type s3` is deprecated; use "
    "`geoseeq link reads` to register S3 files in bulk, or "
    "`geoseeq s3 register` for single staged files."
)


def _maybe_warn_link_type_s3_deprecated(link_type, filepaths):
    """Emit a deprecation warning when ``--link-type s3`` is used with local paths.

    The legitimate "register pre-existing S3 files" workflow now lives in
    ``geoseeq link reads`` (LR-03). The local-file-list form of
    ``upload reads --link-type s3`` is retained for one release with a
    warning before removal.

    No warning is emitted when ``link_type != 's3'`` (e.g. the default
    byte-upload mode) or when the supplied file values are themselves
    ``s3://`` URIs (an edge case kept out of scope by the spec).
    """
    if link_type != 's3':
        return
    values = filepaths.values() if hasattr(filepaths, 'values') else filepaths
    if any(str(v).startswith('s3://') for v in values):
        return
    warnings.warn(
        _LINK_TYPE_S3_DEPRECATION_MSG,
        DeprecationWarning,
        stacklevel=2,
    )
    click.echo(f'DeprecationWarning: {_LINK_TYPE_S3_DEPRECATION_MSG}', err=True)


def _is_fastq(path, fq_exts=['.fastq', '.fq'], compression_exts=['.gz', '.bz2', '']):
    for fq_ext in fq_exts:
        for compression_ext in compression_exts:
            if path.endswith(fq_ext + compression_ext):
                return True
    return False


def _is_fasta(path, fa_exts=['.fasta', '.fa', '.fna', '.faa'], compression_exts=['.gz', '.bz2', '']):
    for fa_ext in fa_exts:
        for compression_ext in compression_exts:
            if path.endswith(fa_ext + compression_ext):
                return True
    return False


def flatten_list_of_fastxs(filepaths):
    """Turn a list of fastq filepaths and txt files containing fastq filepaths into a single list of fastq filepaths."""
    flattened = []
    for path in filepaths:
        if _is_fastq(path) or _is_fasta(path):
            flattened.append(path)
        else:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        flattened.append(line)
    return flattened


def _is_bam(path):
    for ext in ['.bam', '.bai']:
        if path.endswith(ext):
            return True
    return False


def flatten_list_of_bams(filepaths):
    """Turn a list of bam filepaths and txt files containing bam filepaths into a single list of bam filepaths."""
    flattened = []
    for path in filepaths:
        if _is_bam(path):
            flattened.append(path)
        else:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        flattened.append(line)
    return flattened



@click.command('reads')
@use_common_state
@click.option('--cores', default=1, help='Number of uploads to run in parallel')
@overwrite_option
@yes_option
@click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
@private_option
@link_option
@no_new_versions_option
@click.option(
    '--name-map',
    default=None,
    nargs=3,
    required=False,
    help='Optional CSV and column names used to map existing names to new ones. Provide: <file> <current_name_col> <new_name_col>.'
)
@module_option(FASTQ_MODULE_NAMES)
@project_id_arg
@click.argument('fastq_files', type=click.Path(exists=True), nargs=-1)
def cli_upload_reads_wizard(state, cores, overwrite, yes, regex, private, link_type, no_new_versions, name_map, module_name, project_id, fastq_files):
    """Upload fastq read files to GeoSeeq.

    This command automatically groups files by their sample name, lane number
    and read number. It asks for confirmation before creating any samples or
    data.

    ---

    Example Usage:

    \b
    # Upload a list of fastq files to a project, useful if you have hundreds of files
    $ ls -1 path/to/fastq/files/*.fastq.gz > file_list.txt
    $ geoseeq upload reads "GeoSeeq/Example CLI Project" file_list.txt

    \b
    # Upload all the fastq files in a directory to a project
    $ geoseeq upload reads ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/fastq/files/*.fastq.gz

    \b
    # Upload all the fastq files in a directory to a project, performing 4 uploads in parallel
    $ geoseeq upload reads --cores 4 ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/fastq/files/*.fastq.gz

    \b
    # Upload a list of fastq files to a project, automatically creating a new project and overwriting existing files
    $ ls -1 path/to/fastq/files/*.fastq.gz > file_list.txt
    $ geoseeq upload reads --yes --overwrite "GeoSeeq/Example CLI Project" file_list.txt

    \b
    # Remap sample names using a CSV file with current and new names
    $ geoseeq upload reads --name-map sample_map.csv current_name new_name "GeoSeeq/Example CLI Project" fastq_files.txt

    ---

    The optional ``--name-map`` flag takes three values: a CSV filename,
    the column containing current names and the column containing new names.
    When provided, sample names will be translated during upload.

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    \b
    Examples:
     - Name pair: "GeoSeeq/Example CLI Project"
     - UUID: "ed59b913-91ec-489b-a1b9-4ea137a6e5cf"
     - GRN: "grn:gs1:project:ed59b913-91ec-489b-a1b9-4ea137a6e5cf"

    \b
    [FASTQ_FILES...] can be paths to fastq files or a file containing a list of paths, or a mix of both.
    Example: "path/to/fastq/files/*.fastq.gz" "file_list.txt" "path/to/more/fastq/files/*.fastq.gz"

    ---
    """
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id, yes, private)
    filepaths = {basename(line): line for line in flatten_list_of_fastxs(fastq_files)}
    click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    _maybe_warn_link_type_s3_deprecated(link_type, filepaths)
    regex = get_regex(knex, filepaths, module_name, proj, regex)
    groups = group_files(knex, filepaths, module_name, regex, yes, name_map)
    _do_upload(groups, module_name, link_type, proj, filepaths, overwrite, no_new_versions, cores, state)


# @click.command('bam')
# @use_common_state
# @click.option('--genome', default=None, help='The genome aligned to the BAM files. Should be in 2bit format.')
# @click.option('--cores', default=1, help='Number of uploads to run in parallel')
# @overwrite_option
# @yes_option
# @click.option('--regex', default=None, help='An optional regex to use to extract sample names from the file names')
# @private_option
# @link_option
# @no_new_versions_option
# @project_id_arg
# @click.argument('files', type=click.Path(exists=True), nargs=-1)
# def cli_upload_bams(state, genome, cores, overwrite, yes, regex, private, link_type, no_new_versions, project_id, files):
    """Upload BAM files to GeoSeeq.

    This command automatically groups bams with their index files.

    ---

    Example Usage:

    \b
    # Upload a list of BAM files to a project, useful if you have hundreds of files
    $ ls -1 path/to/bam/files/*.bam > file_list.txt
    $ geoseeq upload bams "GeoSeeq/Example CLI Project" file_list.txt

    \b
    # Upload all the BAM files in a directory to a project with BAM indexes
    $ geoseeq upload bams ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/bam/files/*.bam path/to/bam/files/*.bam.bai

    \b
    # Upload all the BAM files in a directory to a project, performing 4 uploads in parallel
    $ geoseeq upload bams --cores 4 ed59b913-91ec-489b-a1b9-4ea137a6e5cf path/to/bam/files/*.bam

    \b
    # Upload a list of BAM files to a project, automatically creating a new project and overwriting existing files
    $ ls -1 path/to/bam/files/*.bam > file_list.txt
    $ geoseeq upload bams --yes --overwrite "GeoSeeq/Example CLI Project" file_list.txt

    ---

    Command Arguments:

    [PROJECT_ID] Can be a project UUID, GeoSeeq Resource Number (GRN), or an
    organization name and project name separated by a slash.

    \b
    Examples:
     - Name pair: "GeoSeeq/Example CLI Project"
     - UUID: "ed59b913-91ec-489b-a1b9-4ea137a6e5cf"
     - GRN: "grn:gs1:project:ed59b913-91ec-489b-a1b9-4ea137a6e5cf"

    \b
    [FILES...] can be paths to BAM files or a file containing a list of paths, or a mix of both.
    Example: "path/to/bam/files
    """
    # knex = state.get_knex()
    # proj = handle_project_id(knex, project_id, yes, private)
    # filepaths = {basename(line): line for line in flatten_list_of_bams(files)}
    # click.echo(f'Found {len(filepaths)} files to upload.', err=True)
    # groups = _group_files(knex, filepaths, 'bam::bam', regex, yes)
    # _do_upload(
    #     groups,
    #     'bam::bam',
    #     link_type,
    #     proj,
    #     filepaths,
    #     overwrite,
    #     no_new_versions,
    #     cores,
    #     state,
    # )

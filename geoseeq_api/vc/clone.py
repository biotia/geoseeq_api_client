from os.path import join, basename
from os import makedirs
import json

from .vc_stub import VCStub
from .vc_sample import VCSample
from .constants import GVCF_EXT


def clone_project(project, target_dir, uuid=False, ext='.gvcf'):
    """Download stub files from a project to local storage.
    
    Return a list of paths to stub files.
    """
    name = project.uuid if uuid else project.name
    root_dir = join(target_dir, name)
    result_dir, sample_dir = join(root_dir, 'results'), join(root_dir, 'samples')
    makedirs(result_dir, exist_ok=True)
    makedirs(sample_dir, exist_ok=True)
    stub_files = []
    for result in project.get_analysis_results():
        stub_files += clone_result(result, result_dir, uuid=uuid, ext=ext, project=True)
    for sample in project.get_samples():
        stub_files += clone_sample(sample, sample_dir, uuid=uuid, ext=ext)
    return stub_files


def clone_sample(sample, target_dir, uuid=False):
    """Download stub files from a sample to local storage.
    
    Return a list of paths to stub files.
    """
    name = sample.uuid if uuid else sample.name
    root_dir = join(target_dir, name)
    makedirs(root_dir, exist_ok=True)
    gsq_dir = join(root_dir, '.geoseeq')
    makedirs(gsq_dir, exist_ok=True)
    vcsample = VCSample(sample, '.')
    vcsample.save_settings(gsq_dir)
    stub_files = []
    for stub, path in vcsample.stubs_from_remote():
        stub_path = path + GVCF_EXT
        stub.save_to_file(stub_path)
        stub_files.append(stub_path)
    return stub_files


def clone_result(result, target_dir, uuid=False, ext='.gvcf', project=False):
    """Download stub files from a result to local storage.
    
    Return a list of paths to stub files.
    """
    name = result.uuid if uuid else result.module_name + '__' + result.replicate
    root_dir = join(target_dir, name)
    makedirs(root_dir, exist_ok=True)
    stub_files = []
    for field in result.get_fields():
        stub_files.append(clone_field(field, root_dir, uuid=uuid, ext=ext, project=project))
    return stub_files


def clone_field(field, target_dir, uuid=False, ext='.gvcf', project=False):
    """Download a stub files from a field to local storage.
    
    Return the path to the stub file.
    """
    name = field.uuid if uuid else field.name
    name += ext
    target_path = join(target_dir, name)
    stub = VCStub(field.brn, field.get_local_filename(), field.checksum())
    stub.save_to_file(target_path)
    return target_path
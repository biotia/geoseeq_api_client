from os.path import makedirs, join, basename
import json


def clone_project(project, target_dir, uuid=False, ext='.gvcf'):
    """Download stub files from a project to local storage.
    
    Return a list of paths to stub files.
    """
    name = project.uuid if uuid else project.name
    root_dir = join(target_dir, name)
    result_dir, sample_dir = join(root_dir, 'results'), join(root_dir, 'samples')
    makedirs(result_dir)
    makedirs(sample_dir)
    stub_files = []
    for result in project.get_analysis_results():
        stub_files += clone_result(result, result_dir, uuid=uuid, ext=ext)
    for sample in project.get_samples():
        stub_files += clone_sample(sample, sample_dir, uuid=uuid, ext=ext)
    return stub_files


def clone_sample(sample, target_dir, uuid=False, ext='.gvcf'):
    """Download stub files from a sample to local storage.
    
    Return a list of paths to stub files.
    """
    name = sample.uuid if uuid else sample.name
    root_dir = join(target_dir, name)
    stub_files = []
    for result in sample.get_analysis_results():
        stub_files += clone_result(result, root_dir, uuid=uuid, ext=ext)
    return root_dir


def clone_result(result, target_dir, uuid=False, ext='.gvcf'):
    """Download stub files from a result to local storage.
    
    Return a list of paths to stub files.
    """
    name = result.uuid if uuid else result.name
    root_dir = join(target_dir, name)
    stub_files = []
    for field in result.get_fields():
        stub_files.append(clone_field(field, root_dir, uuid=uuid, ext=ext))
    return stub_files


def clone_field(field, target_dir, uuid=False, ext='.gvcf'):
    """Download a stub files from a field to local storage.
    
    Return the path to the stub file.
    """
    name = field.uuid if uuid else field.name
    name += ext
    target_path = join(target_dir, name)
    blob = {
        "brn": f'brn:{field.knex.instance_code()}:result_field:{field.uuid}',
        "checksum": field.checksum(),
        "local_path": field.get_local_filename(),
    }
    with open(target_path, 'w') as f_out:
        f_out.write(json.dumps(blob, indent=4))

    return root_dir
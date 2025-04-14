import uuid
from geoseeq.constants import OBJECT_TYPE_STR


def is_grn(el):
    """Return True if `el` is a GeoSeeq Resource Number (GRN)"""
    return el.startswith('grn:')


def is_uuid(el):
    """Return True if `el` is a UUID"""
    try:
        uuid.UUID(el)
        return True
    except ValueError:
        return False


def is_name(el):
    """Return True if `el` is a name.
    
    e.g. "My Org/My Project"
    """
    if "/" in el:
        # every name except org names have a slash
        return True
    if not is_grn_or_uuid(el):
        # if the name has no slash and is not a grn or uuid, it's a name
        return True
    return False


def is_abs_name(el, object_type_str: OBJECT_TYPE_STR) -> bool:
    """Return True if `el` is an absolute name for the given object type."""
    if is_grn_or_uuid(el):
        return False
    n_required_slashes = {
        'org': 0,
        'project': 1,
        'sample': 2,
        'sample_result_folder': 3,
        'sample_result_file': 4,
        'project_result_folder': 2,
        'project_result_file': 3,
    }[object_type_str]
    return el.count('/') == n_required_slashes


def is_grn_or_uuid(el):
    """Return True if `el` is a GRN or a UUID"""
    return is_grn(el) or is_uuid(el)

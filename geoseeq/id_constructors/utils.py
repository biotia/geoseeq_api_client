import uuid


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


def is_grn_or_uuid(el):
    """Return True if `el` is a GRN or a UUID"""
    return is_grn(el) or is_uuid(el)


import json
from glob import glob
from string import Template
from os.path import join

from .vc_stub import VCStub


class VCResultPattern:
    """A class to connect filepaths on the local system to GeoSeeq Results.
    
    Available Patterns:
     - {parent_obj_name}
     - {result_name}
     - {result_replicate} (replicate number of the result)
     - {field_name}
     - {field_checksum} (checksum of the data stored in field)
     - {field_updated_at} (timestamp when the field was last updated)
     - {ext} (file extension)
    """
    DEFAULT_PATTERN = '${parent_obj_name}/${result_name}/${result_name}.${result_replicate}.${field_name}.${ext}'

    def __init__(self, pattern_str):
        self.pattern = pattern_str

    def to_path(self, field):
        """Return the path that should be used to store this field.

        Do not write the field to path.
        """
        template = self.pattern
        if '${parent_obj_name}' in template:
            template = Template(template).safe_substitute(parent_obj_name=field.parent.parent.name)
        if '${result_name}' in template:
            template = Template(template).safe_substitute(result_name=field.parent.module_name)
        if '${result_replicate}' in template:
            template = Template(template).safe_substitute(result_replicate=field.parent.replicate)
        if '${field_name}' in template:
            template = Template(template).safe_substitute(field_name=field.name)
        if '${ext}' in template:
            template = Template(template).safe_substitute(ext=field.get_referenced_filename_ext())
        return template

    def from_path(self, path):
        """Return an AnalysisResult and Field generated from this path.
        
        Do not create the field on the remote or attempt to fetch matching data.
        """
    pass


class VCSample:
    """A class to represent a sample on GeoSeeq via version control.
    
    A version controlled sample can do these things:
     - create stub files locally for new results on the remote
     - create stub files locally for local files that match a particular pattern
    """

    def __init__(self, sample, result_dir, result_pattern=VCResultPattern.DEFAULT_PATTERN):
        self.sample = sample
        self.result_dir = result_dir
        self.result_pattern = VCResultPattern(result_pattern)

    def save_settings(self, geoseeq_dir):
        """Save the setting for this VCSample to disk."""
        blob = {
            "brn": self.sample.brn,  # BRN for the sample
            "results_dir": self.result_dir,  # directory where results are stored
        }
        if self.result_pattern:
            blob["result_pattern"] = self.result_pattern.pattern  # how geoseeq result names should be mapped to filepaths
        else:
            blob["result_pattern"] = VCResultPattern.DEFAULT_PATTERN
        filepath = join(geoseeq_dir, 'sample.json')
        with open(filepath, 'w') as f_out:
            f_out.write(json.dumps(blob, indent=4))

    @classmethod
    def parse_settings(cls, filepath):
        """Return a `VCSample based on the settings filepath."""
        blob = json.loads(open(filepath).read())
        sample = resolve_brn(blob['brn'])
        return cls(
            sample,
            blob['result_dir'],
            result_pattern=blob.get('result_pattern', VCResultPattern.DEFAULT_PATTERN)
        )

    def stubs_from_remote(self):
        for result in self.sample.get_analysis_results():
            for field in result.get_fields():
                path = self.result_pattern.to_path(field)
                stub = VCStub(field.brn, path, field.checksum())
                stub.set_parent_info(
                    self.sample.brn,
                    result.module_name,
                    result.replicate
                )
                yield stub, path

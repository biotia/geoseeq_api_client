class SmartTree:

    def __init__(self, result_file):
        self.result_file = result_file  # the result file that contains the smart tree
        self.content = None

    def create_from_newick(self, newick_str, sample_name_id_map=None):
        self.content = {
            "__type__": "phylodynamics",
            "version": "1.0",
            "tree": {
                "value": newick_str,
                "kind": "newick",
                "sampleNameIdMap": {} if sample_name_id_map is None else sample_name_id_map,
            }
        }
        return self
    
    def add_sample_to_map(self, sample):
        if self.content is None:
            raise ValueError("Must create tree before adding samples")
        self.content["tree"]["sampleNameIdMap"][sample.name] = sample.uuid
        return self
    
    def add_all_samples_to_map(self, project):
        for sample in project.get_samples():
            self.add_sample_to_map(sample)

    @classmethod
    def from_blob(cls, result_file, blob):
        smart_tree = cls(result_file)
        smart_tree.content = blob
        return smart_tree
    
    def save(self):
        if self.content is None:
            raise ValueError("Must create tree before saving")
        self.result_file.upload_json(self.content)
        return self
    
    def idem(self):
        if self.content is None:
            raise ValueError("Must create tree before saving")
        self.result_file.upload_json(self.content)
        return self
    
    def get(self):
        self.result_file.get()
        self.content = self.result_file.stored_data
        return self
    
    def create(self):
        if self.content is None:
            raise ValueError("Must create tree before saving")
        self.result_file.upload_json(self.content)
        return self
        
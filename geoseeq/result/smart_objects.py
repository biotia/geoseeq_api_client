

class ResultFileSmartObjects:
    """Interface for ResultFiles to easily create and use smart objects like tables"""

    def is_smart_table(self):
        """Return True iff this ResultFile is a smart table."""
        return self.stored_data.get("__type__") == "smartTable"

    def as_smart_table(self, description=""):
        """Return a SmartTable object for this ResultFile. Will create a new SmartTable if it doesn't exist yet."""
        if self.exists() and not self.is_smart_table():
            raise ValueError("ResultFile exists but is not a smart table")
        from geoseeq.smart_table import SmartTable  # import here to avoid circular import
        if self.exists():  # already a smart table
            smart_table = SmartTable(self.knex, self.name, connected_file_id=self.uuid).get()
        else:  # create a new smart table
            smart_table = SmartTable(self.knex, self.name, connected_file_id=self.uuid, description=description)
            smart_table.create(self.parent)
        return smart_table

    def is_smart_tree(self):
        """Return True iff this ResultFile is a smart tree."""
        return self.stored_data.get("__type__") == "phylodynamics"
    
    def as_smart_tree(self, newick_str=None, sample_name_id_map=None):
        """Return a SmartTree object for this ResultFile. Will create a new SmartTree if it doesn't exist yet."""
        if self.exists() and not self.is_smart_tree():
            raise ValueError("ResultFile exists but is not a smart tree")
        from geoseeq.smart_tree import SmartTree  # import here to avoid circular import
        if self.exists(): # already a smart tree
            smart_tree = SmartTree.from_blob(self, self.stored_data)
        else:  # create a new smart tree
            smart_tree = SmartTree(self)
            if newick_str is not None:
                smart_tree.create_from_newick(newick_str, sample_name_id_map)
                smart_tree.create()
        return smart_tree

        

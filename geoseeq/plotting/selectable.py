
class PlotSelector:

    def __init__(self, title, kind='dropdown', options=None):
        self.title = title
        assert kind in ['dropdown', 'radio']
        self.kind = kind
        self.options = options or []

    def __iter__(self):
        return iter(self.options)

    def add_option(self, option):
        if option not in self.options:
            self.options.append(option)

    def to_dict(self):
        assert self.options
        return {
            'title': self.title,
            'kind': self.kind,
            'options': self.options,
        }


class SelectablePlot:
    VERSION = 'v0'

    def __init__(self):
        self.selectors = []
        self.data = {}
    
    def add_selector(self, title, kind='dropdown', options=None):
        """Add a selector to the selectable plot. Return the selectable plot for chaining."""
        self.selectors.append(PlotSelector(title, kind=kind, options=options))
        return self

    def _add_keys(self, *keys):
        """Add keys to the data structure."""
        assert self.selectors
        assert len(keys) == len(self.selectors)
        subdata = self.data
        for i, key in enumerate(keys):
            if key not in subdata:
                subdata[key] = {}
                self.selectors[i].add_option(key)
            subdata = subdata[key]
    
    def add_plot(self, plot, *keys):
        """Add a plot to the selectable plot. Return the selectable plot for chaining."""
        self._add_keys(*keys)
        subdata = self.data
        for key in keys:
            if key == keys[-1]:
                subdata[key] = plot
            subdata = subdata[key]
        return self
    
    def _check_invariants(self):
        """Ensure we have plots for each key combo."""
        assert self.selectors
        
        def check_subdata(i, subdata, key_chain):
            if i == len(self.selectors):
                assert subdata, f"subdata does not exist or is empty. Key Chain: {key_chain}"
                return
            for key in self.selectors[i]:
                if key not in subdata:
                    assert False, f'key: "{key}" is missing in `data`'
                check_subdata(i + 1, subdata[key], key_chain + [key])
        
        check_subdata(0, self.data, [])

    def to_dict(self):
        """Return the selectable plot as a dictionary."""
        self._check_invariants()
        return {
            'version': self.VERSION,
            'selectors': [s.to_dict() for s in self.selectors],
            'data': self.data,
        }
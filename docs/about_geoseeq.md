## Geoseeq Data Model

Geoseeq is based on a hierarchical data model with three components:

```
Sample Group -> Sample -> Analysis Result
```

- `Analysis Results` store actual data, the results of analyses and raw data (e.g short reads)
- `Samples` represent biological samples and group results
- `Sample Groups` are projects that group multiple samples

In practice this model includes additional components: `Result Fields` and `Analysis Results` for groups

```
Sample Group -> Sample -> Analysis Result -> Result Field
             |-> Analysis Result -> Result Field
```

- `Result Fields` provide a useful way to organize results with multiple files (e.g. the forward and reverse reads in paired end sequencing)
- `Group Analysis Results` make it possible to store results computed across many samples (e.g. a UMAP reduction)

Finally, some additonal components allow for privacy, permissions, and organization

```
User <-> Organization -> Sample Group -> Sample -> ...
                      |-> Analysis Result -> ...
```

- `Users` are people signed up on Geoseeq
- `Organizations` represent groups (e.g. a Lab). Organizations own groups and set permissions

### Storing Data on Geoseeq

Data is stored in `AnalysisResultFields`, attached to either `Samples` or `SampleGroups`. Each `AnalysisResultField` contains a JSON blob. This blob may be the data itself or, often, is a pointer to data stored somewhere else on the internet like `S3` or `FTP`.

If the stored data is anything other than a plain JSON blob it should include a special field `__type__` specifying how it should be handled.



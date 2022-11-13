# Simple Python Example

This is a very simple example of analyzing data from [Geoseeq](https://portal.geoseeq.com). This script does the following:

1. Gets a list of samples in a Geoseeq group
2. Checks if the samples have already been processed
3. Downloads read from samples that have not been processed
4. Processes the reads
5. Uploads the new result

To run the example you will need to have the [geoseeq python api](https://github.com/biotia/geoseeq_api_client) installed. If you are not using the demo group (below) you will need a [Geoseeq](https://portal.geoseeq.com) login and a sample group to process.

```
python simple_python_example.py --help
```

```
python simple_python_example.py -e <your login email> -p <your password> <your organization name> <your group name>
```

TODO: Create demo sample group and user and add uuids and api-token here

Alternatively you can run the example on the [Geoseeq Demo Group](https://portal.geoseeq.com/sample-groups/6c0f8eea-b183-47af-84df-99ab33d4292b) using the [Demo User](https://portal.geoseeq.com/users/7fb84c58-933a-49e5-92ca-b25fa3360ff6)

```
python simple_python_example.py -a <api_token_has_to_be_created> 'Geoseeq Demo' 'Simple Demo Group'
```

When you run this command you should see similar output to this

```
Using result module name: example::simple_python_example_224665
Processing sample Sample 1...
Sample Sample 1 has not been processed
Calculating stats for Sample 1
```

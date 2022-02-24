# Simple Python Example

This is a very simple example of analyzing data from [Pangea](https://pangeabio.io). This script does the following:

1. Gets a list of samples in a Pangea group
2. Checks if the samples have already been processed
3. Downloads read from samples that have not been processed
4. Processes the reads
5. Uploads the new result

To run the example you will need to have the [pangea python api](https://github.com/LongTailBio/pangea-django/tree/master/api-client) installed. If you are not using the demo group (below) you will need a [Pangea](https://pangeabio.io) login and a sample group to process.

```
python simple_python_example.py --help
```

```
python simple_python_example.py -e <your login email> -p <your password> <your organization name> <your group name>
```

Alternatively you can run the example on the [Pangea Demo Group](https://pangeabio.io/sample-groups/6c0f8eea-b183-47af-84df-99ab33d4292b) using the [Demo User](https://pangeabio.io/users/7fb84c58-933a-49e5-92ca-b25fa3360ff6)

```
python simple_python_example.py -e demo_user@pangeabio.io -p demo_user_password 'Pangea Demo' 'Simple Demo Group'
```

When you run this command you should see similar output to this
```
Using result module name: example::simple_python_example_224665
Processing sample Sample 1...
Sample Sample 1 has not been processed
Calculating stats for Sample 1
```
import click
import pandas as pd

from geoseeq_api import Knex, PipelineRun

data = {
    "pipeline": "c9be07e8-88fb-455a-9bfb-6d9b31a34bd0",
    "sample_group": "e93d2d5b-3757-41a7-a840-5b45890f6d7c",
    "pipeline_version": "1.0",
}


@click.command()
@click.option("--token", default=None, envvar="GEOSEEQ_TOKEN")
@click.option("--url", default="http://localhost:8000")
def main(token, url):
    k = Knex(url)
    k.add_api_token(token)

    run = PipelineRun(k, **data).create()
    run.status = "running"
    run.save()


if __name__ == "__main__":
    main()

from typing import Optional
import click
import ee

import bootstrap  # noqa
from config.model_settings import DataConfig, OSMConfig
from src.load_ee_data import LoadEEData
from src.generate_biilding_centroids import GenerateBuildingCentroids

# from cli.load_ee_cli import load_data_options


class LoadDataFlow:
    def __init__(self):
        self.settings = DataConfig

    def execute(self):
        # Trigger the authentication flow.
        ee.Authenticate()

        config = DataConfig()
        data_loader = LoadEEData.from_dataclass_config(config)

        data_loader.execute()


class GenerateBuildingCentroidsFlow:
    def __init__(self):
        self.data_settings = DataConfig
        self.osm_settings = OSMConfig

    def execute(self):
        data_config = DataConfig()
        osm_config = OSMConfig()
        data_loader = GenerateBuildingCentroids.from_dataclass_config(
            data_config, osm_config
        )

        data_loader.execute()


@click.command("generate_biilding_centroids", help="Retrieve building centroids")
def generate_building_centroids():
    GenerateBuildingCentroidsFlow().execute()


@click.command("load_data", help="Load data from Google Earth Engine")
def load_data():
    LoadDataFlow().execute()


@click.group("reinventing-catastrophe-modelling", help="Run full analysis pipeline")
@click.pass_context
def cli(ctx):
    ...


cli.add_command(generate_building_centroids)
cli.add_command(load_data)

if __name__ == "__main__":
    cli()

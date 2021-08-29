from typing import Optional
import click
import ee

import bootstrap  # noqa
from config.model_settings import DataConfig, OSMConfig
from src.load_ee_data import LoadEEData
from src.generate_building_centroids import GenerateBuildingCentroids

# from cli.load_ee_cli import load_data_options


class LoadDataFlow:
    def __init__(self):
        self.config = DataConfig()

    def execute(self):
        # Trigger the authentication flow.
        ee.Authenticate()
        data_loader = LoadEEData.from_dataclass_config(self.config)

        data_loader.execute()

    def execute_for_country(self, building_footprint_gdf):
        # Trigger the authentication flow.
        ee.Authenticate()
        data_loader = LoadEEData.from_dataclass_config(self.config)

        return data_loader.execute_for_country(building_footprint_gdf)


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

        return data_loader.execute()


@click.command("generate_building_centroids", help="Retrieve building centroids")
def generate_building_centroids():
    GenerateBuildingCentroidsFlow().execute()


@click.command("load_data", help="Load data from Google Earth Engine")
def load_data():
    LoadDataFlow().execute()


@click.command("run_pipeline", help="Run full analysis pipeline")
def run_full_pipeline():
    building_footprint_gdf = GenerateBuildingCentroidsFlow().execute()
    time_series_df = LoadDataFlow().execute_for_country(building_footprint_gdf)
    print(time_series_df)


@click.group(
    "reinventing-catastrophe-modelling",
    help="Library aiming to reinvent catastrophe modelling using a combination of satellite data and urban analytics techniques",
)
@click.pass_context
def cli(ctx):
    ...


cli.add_command(generate_building_centroids)
cli.add_command(load_data)
cli.add_command(run_full_pipeline)

if __name__ == "__main__":
    cli()

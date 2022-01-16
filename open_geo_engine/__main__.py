from typing import Optional
import click
import ee

import bootstrap  # noqa
from config.model_settings import DataConfig, OSMConfig, StreetViewConfig
from open_geo_engine.src.load_ee_data import LoadEEData
from open_geo_engine.src.generate_building_centroids import GenerateBuildingCentroids
from open_geo_engine.src.get_google_streetview import GetGoogleStreetView


class GenerateBuildingCentroidsFlow:
    def __init__(self):
        self.data_settings = DataConfig
        self.osm_settings = OSMConfig

    def execute(self):
        data_config = DataConfig()
        osm_config = OSMConfig()
        building_generator = GenerateBuildingCentroids.from_dataclass_config(
            data_config, osm_config
        )

        return building_generator.execute()


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


class GetGoogleStreetViewFlow:
    def __init__(self):
        self.streetview_config = StreetViewConfig()

    def execute(self):
        streetview_downloader = GetGoogleStreetView.from_dataclass_config(
            self.streetview_config
        )

        return streetview_downloader.execute()

    def execute_for_country(self, satellite_data_df):
        # Trigger the authentication flow.
        ee.Authenticate()
        streetview_downloader = GetGoogleStreetView.from_dataclass_config(
            self.streetview_config
        )

        return streetview_downloader.execute_for_country(satellite_data_df)


@click.command("generate_building_centroids", help="Retrieve building centroids")
def generate_building_centroids():
    building_footprint_gdfs = GenerateBuildingCentroidsFlow().execute()
    print(building_footprint_gdfs)
    building_footprint_gdfs.to_csv(f"local_data/{StreetViewConfig.PLACE}_school.csv")


@click.command("load_data", help="Load data from Google Earth Engine")
def load_data():
    LoadDataFlow().execute()


@click.command(
    "get_google_streetview", help="Retrieve streetview images for building locations"
)
def get_google_streetview():
    GetGoogleStreetViewFlow().execute()


@click.command("run_pipeline", help="Run full analysis pipeline")
def run_full_pipeline():
    building_footprint_gdf = GenerateBuildingCentroidsFlow().execute()
    satellite_data_df = LoadDataFlow().execute_for_country(building_footprint_gdf)
    print(satellite_data_df)
    GetGoogleStreetViewFlow().execute_for_country(satellite_data_df)


@click.group(
    "open-geo-engine",
    help="Library aiming to integrate disparate satellite, and geospatial datasets for pollution modelling",
)
@click.pass_context
def cli(ctx):
    ...


cli.add_command(generate_building_centroids)
cli.add_command(load_data)
cli.add_command(get_google_streetview)
cli.add_command(run_full_pipeline)

if __name__ == "__main__":
    cli()

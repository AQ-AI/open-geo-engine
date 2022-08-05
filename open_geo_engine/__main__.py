import click
import ee

from open_geo_engine.config.model_settings import (
    DataConfig,
    OSMConfig,
    StreetViewConfig,
)
from open_geo_engine.src.generate_building_centroids import GenerateBuildingCentroids
from open_geo_engine.src.get_google_streetview import GetGoogleStreetView
from open_geo_engine.src.load_ee_data import LoadEEData


class GenerateBuildingCentroidsFlow:
    def __init__(self):
        self.data_settings = DataConfig()
        self.osm_settings = OSMConfig()

    def execute(self):
        building_generator = GenerateBuildingCentroids.from_dataclass_config(
            self.data_settings, self.osm_settings
        )
        return building_generator.execute()


class LoadDataFlow:
    def __init__(self):
        self.config = DataConfig()

    def execute(self, building_footprint_gdf):
        # Trigger the authentication flow.
        ee.Authenticate()
        data_loader = LoadEEData.from_dataclass_config(self.config)

        data_loader.execute(building_footprint_gdf, save_images=False)

    def execute_for_country(self):
        # Trigger the authentication flow.
        ee.Authenticate()
        data_loader = LoadEEData.from_dataclass_config(self.config)

        return data_loader.execute_for_country(save_images=False)


class GetGoogleStreetViewFlow:
    def __init__(self):
        self.streetview_config = StreetViewConfig()

    def execute_for_df(self, satellite_data_df):
        # Trigger the authentication flow.
        ee.Authenticate()
        streetview_downloader = GetGoogleStreetView.from_dataclass_config(
            self.streetview_config
        )

        return streetview_downloader.execute_for_df(satellite_data_df)


@click.command("generate_building_centroids", help="Retrieve building centroids")
def generate_building_centroids():
    GenerateBuildingCentroidsFlow().execute()


@click.command("load_data", help="Load data from Google Earth Engine")
def load_data():
    LoadDataFlow().execute()


@click.command(
    "get_google_streetview", help="Retrieve streetview images for building locations"
)
def get_google_streetview(satellite_data_df):
    GetGoogleStreetViewFlow().execute_for_country(satellite_data_df)


@click.command("run_pipeline", help="Run full analysis pipeline")
@click.argument("path_to_local_data")
def run_full_pipeline(path_to_local_data):
    building_footprint_gdf = GenerateBuildingCentroidsFlow().execute()
    building_footprint_gdf.to_csv(
        f"{path_to_local_data}/osm_data/building_footprint.csv"
    )
    satellite_data_df = LoadDataFlow().execute(building_footprint_gdf)
    # GetGoogleStreetViewFlow().execute_for_df(satellite_data_df)


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

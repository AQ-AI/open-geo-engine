import click
import ee
from shapely import wkt
import pandas as pd
import geopandas as gpd
import collections

collections.Callable = collections.abc.Callable
from open_geo_engine.config.model_settings import (
    DataConfig,
    OSMConfig,
    StreetViewConfig,
    SatelliteTemporalAggregatorConfig,
    PollutionJoinerConfig,
)
from open_geo_engine.src.generate_building_centroids import GenerateBuildingCentroids
from open_geo_engine.src.get_google_streetview import GetGoogleStreetView
from open_geo_engine.src.load_ee_data import LoadEEData
from open_geo_engine.src.satellite_temporal_aggregator import SatelliteTemporalAggregator
from open_geo_engine.src.pollution_joiner import PollutionJoiner


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
        # self.filepath = filepath

    def execute(self, building_footprint_gdf):
        # Trigger the authentication flow.
        data_loader = LoadEEData.from_dataclass_config(self.config)

        data_loader.execute(building_footprint_gdf, save_images=True)

    def execute_for_country(self, building_footprint_gdf):
        # Trigger the authentication flow.
        data_loader = LoadEEData.from_dataclass_config(self.config)

        return data_loader.execute_for_country(building_footprint_gdf, save_images=False)


class SatelliteTemporalAggregatorFlow:
    def __init__(self):
        self.satellite_agg_config = SatelliteTemporalAggregatorConfig()

    def execute(self):
        satellite_temporal_aggregator = SatelliteTemporalAggregator.from_dataclass_config(
            self.satellite_agg_config
        )
        return satellite_temporal_aggregator.execute()


class PollutionJoinerFlow:
    def __init__(self):
        self.pollution_joiner_config = PollutionJoinerConfig()

    def execute(self):
        pollution_joiner = PollutionJoiner.from_dataclass_config(self.pollution_joiner_config)
        return pollution_joiner.execute()


class GetGoogleStreetViewFlow:
    def __init__(self):
        self.streetview_config = StreetViewConfig()

    def execute_for_country(self, satellite_data_df):
        # Trigger the authentication flow.
        ee.Authenticate()
        streetview_downloader = GetGoogleStreetView.from_dataclass_config(self.streetview_config)

        return streetview_downloader.execute_for_country(satellite_data_df)


@click.command("generate_building_centroids", help="Retrieve building centroids")
def generate_building_centroids():
    osm_df = GenerateBuildingCentroidsFlow().execute()
    osm_df.to_csv(
        "/home/ubuntu/unicef_work/open-geo-engine/local_data/osm_data/saudi_arabia_all.csv"
    )


@click.command("load_data", help="Load data from Google Earth Engine")
def load_data():
    LoadDataFlow().execute()


@click.command("join_satellite_data", help="Join Satelliteand air pollution  data temporally")
def join_satellite_data():
    SatelliteTemporalAggregatorFlow().execute()
    PollutionJoinerFlow().execute()


@click.command("get_google_streetview", help="Retrieve streetview images for building locations")
def get_google_streetview(satellite_data_df):
    GetGoogleStreetViewFlow().execute_for_country(satellite_data_df)


@click.command("run_pipeline", help="Run full analysis pipeline")
def run_full_pipeline():
    # building_footprint_gdf = GenerateBuildingCentroidsFlow().execute()
    # print(building_footprint_gdf)
    ee.Initialize()
    sensor_locations_df = pd.read_csv(
        "/home/ubuntu/unicef_work/open-geo-engine/local_data/locations_dehli.csv"
    )
    gdf = gpd.GeoDataFrame(
        sensor_locations_df,
        geometry=gpd.points_from_xy(sensor_locations_df.x, sensor_locations_df.y),
    )
    satellite_data_df = LoadDataFlow().execute_for_country(gdf)
    satellite_data_df.to_csv(
        "/home/ubuntu/unicef_work/open-geo-engine/local_data/dehli_sensors_sat_no2.csv"
    )
    # GetGoogleStreetViewFlow().execute_for_country(satellite_data_df)


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
cli.add_command(join_satellite_data)

if __name__ == "__main__":
    cli()

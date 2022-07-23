import pandas as pd
import pytest
from pydantic.dataclasses import dataclass

from open_geo_engine.__main__ import (
    GenerateBuildingCentroidsFlow,
    GetGoogleStreetViewFlow,
    LoadDataFlow,
    generate_building_centroids,
    get_google_streetview,
    load_data,
    run_full_pipeline,
)


@dataclass
class OSMTestConfig:
    tags = {"leisure": "park"}
    place = "Parque El Retiro, Madrid"


@dataclass
class DataTestConfig:
    country_codes = ["ES"]
    country_bounding_boxes = {
        "ES": (
            "Spain",
            (-9.39288367353, 35.946850084, 3.03948408368, 43.7483377142),
        )
    }
    place = "Parque El Retiro, Madrid"
    year = 2020
    mon_start = 1
    date_start = 1
    year_end = 2021
    mon_end = 1
    date_end = 1
    image_collection = "LANDSAT/LC08/C01/T1"
    image_band = ["B4", "B3", "B2"]
    folder = "/ee_data"
    model_name = "LANDSAT"


@dataclass
class StreetviewTestConfig:
    size = "600x300"
    heading = "151.78"
    pitch = "-0.76"
    KEY = "a-random-string"
    local_image_folder = "tests/test_data"
    local_links_folder = "tests/test_data"
    local_metadata_folder = "tests/test_data"
    PLACE = "Parque_El_Retiro_Madrid"
    META_BASE = "https://maps.googleapis.com/maps/api/streetview/metadata?"


@pytest.fixture(params=[OSMTestConfig, DataTestConfig, StreetviewTestConfig])
def test_main_flow(OSMTestConfig, DataTestConfig):
    osm_config = OSMTestConfig()
    data_config = DataTestConfig()
    streetview_config = StreetviewTestConfig()
    satellite_data_df = pd.DataFrame(
        {
            "longitude": [-3.683317243711068, -3.683317243711068],
            "latitude": [40.41498005371624, 40.41498005371624],
            "time": [1578653746335, 1580036142137],
            "datetime": ["2020-01-10 10:55:46.335,", "2020-01-26 10:55:42.137"],
            "B4": [7053, 6869],
            "B3": [7177, 7069],
            "B2": [7825, 7720],
        }
    )

    generate_building_centroid_flow = GenerateBuildingCentroidsFlow(
        osm_config, data_config
    )
    assert generate_building_centroid_flow().execute().shape == (1, 21)
    data_config = DataTestConfig()

    load_data_flow = LoadDataFlow(data_config)
    assert load_data_flow().execute().shape == (2, 7)

    get_google_streetview_flow = GetGoogleStreetViewFlow(streetview_config)
    assert (
        len(get_google_streetview_flow().execute_for_country(satellite_data_df).columns)
        == 10
    )


@pytest.fixture(
    params=[
        run_full_pipeline,
        generate_building_centroids,
        load_data,
        get_google_streetview,
    ]
)
def test_cli(run_full_pipeline):
    assert print(type(run_full_pipeline).__name__) == "Command"
    assert print(type(generate_building_centroids).__name__) == "Command"
    assert print(type(load_data).__name__) == "Command"
    assert print(type(get_google_streetview).__name__) == "Command"

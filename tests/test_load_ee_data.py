from open_geo_engine.src.load_ee_data import LoadEEData
from open_geo_engine.src.generate_building_centroids import GenerateBuildingCentroids


def test_prepare_dates():
    countries = ["ES"]
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
    image_folder = "/ee_data"
    model_name = "LANDSAT"
    load_ee_data = LoadEEData(
        countries,
        year,
        mon_start,
        date_start,
        year_end,
        mon_end,
        date_end,
        image_collection,
        image_band,
        folder,
        image_folder,
        model_name,
        place,
    )

    assert len(load_ee_data._generate_start_end_date()) == 2


def test_get_xy():
    countries = ["ES"]
    place = "Parque El Retiro, Madrid"
    tags = {"leisure": "park"}

    generate_building_centroids = GenerateBuildingCentroids(countries, place, tags)
    buildings_gdf = generate_building_centroids.execute()

    year = 2020
    mon_start = 1
    date_start = 1
    year_end = 2020
    mon_end = 1
    date_end = 31
    image_collection = "LANDSAT/LC08/C01/T1"
    image_band = ["B4", "B3", "B2"]
    folder = "/test_data"
    image_folder = "/test_data"
    model_name = "LANDSAT"

    load_ee_data = LoadEEData(
        countries,
        year,
        mon_start,
        date_start,
        year_end,
        mon_end,
        date_end,
        image_collection,
        image_band,
        folder,
        image_folder,
        model_name,
        place,
    )
    xy_df = load_ee_data._get_xy(buildings_gdf)[["x", "y"]]
    assert xy_df["x"][0] == -3.68328454639349
    assert xy_df["y"][0] == 40.41494595

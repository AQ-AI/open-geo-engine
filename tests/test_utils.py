import datetime
from unittest.mock import patch

from pandas._testing import assert_frame_equal

from open_geo_engine.src.generate_building_centroids import GenerateBuildingCentroids
from open_geo_engine.src.load_ee_data import LoadEEData


def test_ee_array_to_df():
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
    expected_date_list = ["2020-01-01", "2020-01-08", "2020-01-15", "2020-01-22", "2020-01-29"]

    with patch("open_geo_engine.src.load_ee_data.LoadEEData._get_xy") as get_xy:
        get_xy.return_value = buildings_gdf
        assert_frame_equal(
            LoadEEData(
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
            )._get_xy(),
            buildings_gdf,
        )

        start, end = (
            datetime.datetime(2020, 1, 1, 0, 0),
            datetime.datetime(2020, 1, 31, 0, 0),
        )
        date_list = LoadEEData(
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
        )._date_range(start, end)

        with patch("open_geo_engine.src.load_ee_data.LoadEEData._generate_dates") as generate_dates:
            generate_dates.return_value = [
                "2020-01-01",
                "2020-01-08",
                "2020-01-15",
                "2020-01-22",
                "2020-01-29",
            ]
            assert (
                LoadEEData(
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
                )._generate_dates(date_list)
                == expected_date_list
            )

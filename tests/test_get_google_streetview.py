import os

import pandas as pd

from open_geo_engine.src.get_google_streetview import GetGoogleStreetView


def test_get_google_streetview():
    size = "600x300"
    heading = "151.78"
    pitch = "-0.76"
    key = os.environ.get("GOOGLE_DEV_API_KEY")
    image_folder = "tests/test_data"
    links_file = "tests/test_data/streetview_links.txt"
    metadata_file = "tests/test_data/test_metadata.json"
    place = "Parque_El_Retiro_Madrid"
    meta_base = "https://maps.googleapis.com/maps/api/streetview/metadata?"
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

    get_google_streetview = GetGoogleStreetView(
        size,
        heading,
        pitch,
        key,
        image_folder,
        links_file,
        metadata_file,
        place,
        meta_base,
    )

    assert (
        get_google_streetview.generate_lat_lon_string(satellite_data_df)
        == "40.41498005371624,-3.683317243711068"
    )
    lat_lon_str = get_google_streetview.generate_lat_lon_string(satellite_data_df)
    params = get_google_streetview._generate_params(lat_lon_str)

    satellite_data_df["lat_lon_str"] = get_google_streetview._join_lat_lon(satellite_data_df)
    assert satellite_data_df["lat_lon_str"][0] == str(lat_lon_str)
    assert (
        get_google_streetview.add_metadata_to_satellite_df(satellite_data_df)["metadata"][0]
        == "<Response [200]>"
    )

    params.pop("key")

    assert params == {
        "size": "600x300",
        "location": "40.41498005371624,-3.683317243711068",
        "pitch": "-0.76",
    }
    satellite_streetview_data_df = get_google_streetview.add_links_to_satellite_df(
        satellite_data_df
    )

    assert satellite_streetview_data_df["latitude"][0] == 40.41498005371624
    assert satellite_streetview_data_df["longitude"][0] == -3.683317243711068

    assert len(satellite_streetview_data_df.columns) == 10

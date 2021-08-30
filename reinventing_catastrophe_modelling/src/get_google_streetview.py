import json
from pathlib import Path
import requests
import google_streetview.api
import google_streetview.helpers
import pandas as pd

from config.model_settings import StreetViewConfig
from utils.utils import write_csv


class GetGoogleStreetView:
    def __init__(
        self,
        size: str,
        heading: str,
        pitch: str,
        key: str,
        image_folder: str,
        links_folder: str,
        metadata_folder: str,
        place: str,
        meta_base: str,
    ):
        self.size = size
        self.heading = heading
        self.pitch = pitch
        self.key = key
        self.image_folder = image_folder
        self.links_folder = links_folder
        self.metadata_folder = metadata_folder
        self.place = place
        self.meta_base = meta_base

    @classmethod
    def from_dataclass_config(
        cls, streetview_config: StreetViewConfig
    ) -> "GetGoogleStreetView":

        return cls(
            size=streetview_config.SIZE,
            heading=streetview_config.HEADING,
            pitch=streetview_config.PITCH,
            key=streetview_config.KEY,
            image_folder=streetview_config.LOCAL_IMAGE_FOLDER,
            links_folder=streetview_config.LOCAL_LINKS_FOLDER,
            metadata_folder=streetview_config.LOCAL_METADATA_FOLDER,
            place=streetview_config.PLACE,
            meta_base=streetview_config.META_BASE,
        )

    def execute_for_country(self, satellite_data_df):
        lat_lon_str = self.generate_lat_lon_string(satellite_data_df)
        params = self._generate_params(lat_lon_str)
        results = self.get_google_streetview(google_streetview.helpers.api_list(params))
        self.save_streetview_information(results)

        satellite_streetview_data_df = self.add_links_to_satellite_df(satellite_data_df)

        satellite_streetview_metadata_df = self.add_metadata_to_satellite_df(
            satellite_streetview_data_df
        )

        write_csv(
            satellite_streetview_metadata_df,
            f"{Path(__file__).resolve().parent.parent.parent}/local_data/{self.place}.csv",
        )

    def generate_lat_lon_string(self, satellite_data_df):
        satellite_lat_lon_unique = satellite_data_df[
            ["latitude", "longitude"]
        ].drop_duplicates()
        satellite_lat_lon_unique["lat_lon_str"] = self._join_lat_lon(
            satellite_lat_lon_unique
        )
        return ";".join(satellite_lat_lon_unique["lat_lon_str"])

    def get_google_streetview(self, params):
        return google_streetview.api.results(params)

    def save_streetview_information(self, results):
        results.download_links(self.image_folder)
        results.save_links(f"{self.links_folder}/streetview_links.txt")
        results.save_metadata(f"{self.metadata_folder}/streetview_metadata.json")

    def add_links_to_satellite_df(self, satellite_data_df):
        satellite_data_df["lat_lon_str"] = self._join_lat_lon(satellite_data_df)
        street_view_links_df = pd.read_csv(
            f"{self.links_folder}/streetview_links.txt", sep="\n", names=["URL"]
        )
        street_view_links_df["latitude"] = street_view_links_df["URL"].str.extract(
            "location=(.*)%2C"
        )
        street_view_links_df["longitude"] = street_view_links_df["URL"].str.extract(
            "%2C(.*)&pitch"
        )
        street_view_links_df[["latitude", "longitude"]] = street_view_links_df[
            ["latitude", "longitude"]
        ].apply(pd.to_numeric, errors="coerce")
        return satellite_data_df.merge(
            street_view_links_df, on=["latitude", "longitude"]
        )

    def add_metadata_to_satellite_df(self, satellite_data_df):
        for lat_lon in satellite_data_df["lat_lon_str"]:
            meta_params = {"key": self.key, "location": lat_lon}
            satellite_data_df["metadata"] = str(
                requests.get(self.meta_base, params=meta_params)
            )
        return satellite_data_df

    def _join_lat_lon(self, satellite_data_df):
        return satellite_data_df[["latitude", "longitude"]].apply(
            lambda x: ",".join(x.astype(str)), axis=1
        )

    def _generate_params(self, lat_lon_str):
        param_dict = {}
        param_dict["size"] = self.size
        param_dict["location"] = lat_lon_str
        param_dict["pitch"] = self.pitch
        param_dict["key"] = self.key
        return param_dict

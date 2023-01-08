import datetime
from typing import Any, Sequence, Tuple
import logging

import ee
import geemap
import pandas as pd
import shapely
from ee.ee_exception import EEException
from googleapiclient.errors import HttpError
from joblib import Parallel, delayed

from open_geo_engine.config.model_settings import DataConfig
from open_geo_engine.utils.utils import ee_array_to_df


class LoadEEData:
    def __init__(
        self,
        countries: Sequence,
        year: int,
        mon_start: int,
        date_start: int,
        year_end: int,
        mon_end: int,
        date_end: int,
        image_collection: str,
        image_band: str,
        folder: str,
        image_folder: str,
        model_name: str,
        **kwargs,
    ):
        self.countries = countries
        self.year = year
        self.mon_start = mon_start
        self.date_start = date_start
        self.year_end = year_end
        self.mon_end = mon_end
        self.date_end = date_end
        self.image_collection = image_collection
        self.image_band = image_band
        self.folder = folder
        self.image_folder = image_folder
        self.model_name = model_name

    @classmethod
    def from_dataclass_config(cls, config: DataConfig) -> "LoadEEData":
        countries = []
        for country in config.COUNTRY_CODES:
            country_info = config.COUNTRY_BOUNDING_BOXES.get(country, "WO")
            countries.append(country_info)

        return cls(
            countries=countries,
            year=config.YEAR,
            mon_start=config.MON_START,
            date_start=config.DATE_START,
            year_end=config.YEAR_END,
            mon_end=config.MON_END,
            date_end=config.DATE_END,
            image_collection=config.METEROLOGICAL_IMAGE_COLLECTION,
            image_band=config.METEROLOGICAL_IMAGE_BAND,
            folder=config.BASE_FOLDER,
            image_folder=config.IMAGE_FOLDER,
            model_name=config.MODEL_NAME,
            place=config.PLACE,
        )

    def execute(self, save_images):
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_country)(country, save_images)
            for country in self.countries
        )

    def execute_for_country(self, country, save_images):
        logging.info(f"Downloading {self.model_name} data for {country[0]}")
        ee.Initialize()
        coords_tup = country[1]
        s_datetime, e_datetime = self._generate_start_end_date()
        geom = ee.Algorithms.GeometryConstructors.BBox(
            coords_tup[0], coords_tup[1], coords_tup[2], coords_tup[3]
        )

        s_date = s_datetime.date()
        e_date = e_datetime.date()

        collection = (
            ee.ImageCollection(self.image_collection)
            .filterBounds(geom)
            .filterDate(str(s_date), str(e_date))
            .select(self.image_band)
        )

        if save_images:
            self.save_images_to_drive(
                collection, s_datetime, e_datetime, country
            )

        if self.filepath:
            locations_gdf = pd.read_csv(self.filepath)
            # locations_gdf = self._get_xy(locations_gdf)
            locations_ee_list = []
            for lon, lat in zip(locations_gdf.x, locations_gdf.y):
                centroid_point = ee.Geometry.Point(lon, lat)
                satellite_centroid_point = (
                    self._get_centroid_value_from_collection(
                        collection, centroid_point
                    )
                )
                try:
                    ee_df = ee_array_to_df(
                        satellite_centroid_point, self.image_band
                    )
                    if not ee_df.empty:
                        locations_ee_list.append(ee_df)
                except IndexError:
                    pass
            if len(self.countries) == 1:

                locations_ee_df = pd.concat(locations_ee_list)
                locations_ee_df.to_csv(
                    f"local_data/gee_data/{country[0]}_{self.model_name}.csv"
                )
                return locations_ee_df
            else:
                return pd.concat(locations_ee_list)

    def save_images_to_drive(
        self, collection, s_datetime, e_datetime, country
    ):
        s_date = s_datetime.date()
        e_date = e_datetime.date()
        geemap.ee_export_image_collection(
            collection,
            out_dir=f"{self.image_folder}/{self.model_name}_{s_date}_{e_date}_{country}",
        )

    def _get_centroid_value_from_collection(self, collection, centroid_point):
        try:
            return collection.getRegion(centroid_point, 10).getInfo()
        except (EEException, HttpError):
            logging.warning(
                f"""Centroid location {centroid_point}
                table does not match any existing location."""
            )
            pass

    def _generate_start_end_date(self) -> Tuple[datetime.date, datetime.date]:
        start = datetime.datetime(self.year, self.mon_start, self.date_start)
        end = datetime.datetime(self.year_end, self.mon_end, self.date_end)
        return start, end

    def _date_range(self, start, end) -> Sequence[Any]:
        r = (end + datetime.timedelta(days=1) - start).days
        return [start + datetime.timedelta(days=i) for i in range(0, r, 7)]

    def _generate_dates(self, date_list) -> Sequence[str]:
        return [str(date) for date in date_list]

    def _get_xy(self, locations_gdf):
        try:
            locations_gdf["centroid_geometry"] = locations_gdf[
                "centroid_geometry"
            ].map(shapely.wkt.loads)

        except TypeError:
            pass

        locations_gdf["x"] = locations_gdf.centroid_geometry.map(lambda p: p.x)
        locations_gdf["y"] = locations_gdf.centroid_geometry.map(lambda p: p.y)
        return locations_gdf

    def _replace_symbol(self, item):
        return str(item).replace(".", "_")

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
        place: str,
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
        self.place = place

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
            image_collection=config.AOD_IMAGE_COLLECTION,
            image_band=config.AOD_IMAGE_BAND,
            folder=config.BASE_FOLDER,
            image_folder=config.IMAGE_FOLDER,
            model_name=config.MODEL_NAME,
            place=config.PLACE,
        )

    def execute(self, building_footprint_gdf, save_images, **kwargs):
        if kwargs.get("filepath"):
            building_footprint_gdf = pd.read_csv(kwargs.get("filepath"))
        building_footprints_satellite_df = Parallel(
            n_jobs=-1, backend="multiprocessing", verbose=5
        )(
            delayed(self.execute_for_country)(
                building_footprint_gdf, country, save_images
            )
            for country in self.countries
        )
        return building_footprints_satellite_df

    def execute_for_country(self, building_footprint_gdf, country, save_images):
        logging.info(f"Downloading {self.model_name} data for {self.place}")
        ee.Initialize()
        coords_tup = country[1]
        s_datetime, e_datetime = self._generate_start_end_date()
        geom = ee.Algorithms.GeometryConstructors.BBox(
            coords_tup[0], coords_tup[1], coords_tup[2], coords_tup[3]
        )
        collection = (
            ee.ImageCollection(self.image_collection)
            .filterBounds(geom)
            .select(self.image_band)
            .filterDate(s_datetime, e_datetime)
        )
        if save_images is True:
            self.save_images_to_drive(collection, s_datetime, e_datetime, country)

        building_footprint_gdf = self._get_xy(building_footprint_gdf)
        osm_ee_list = []
        for lon, lat in zip(building_footprint_gdf.x, building_footprint_gdf.y):
            centroid_point = ee.Geometry.Point(lon, lat)
            landsat_centroid_point = self._get_centroid_value_from_collection(
                collection, centroid_point
            )

            ee_df = ee_array_to_df(landsat_centroid_point, self.image_band)
            if not ee_df.empty:
                osm_ee_list.append(ee_df)
        if len(self.countries) == 1:
            osm_ee_df = pd.concat(osm_ee_list)
            osm_ee_df.to_csv(f"local_data/gee_data/{country[0]}_{self.model_name}.csv")
            return osm_ee_df
        else:
            return pd.concat(osm_ee_list)

    def save_images_to_drive(self, collection, s_datetime, e_datetime, country):
        s_date = s_datetime.date()
        e_date = e_datetime.date()
        geemap.ee_export_image_collection(
            collection,
            out_dir=f"{self.image_folder}/{self.model_name}_{s_date}_{e_date}_{country[0]}",
        )

    def _get_centroid_value_from_collection(self, collection, centroid_point):
        try:
            return collection.getRegion(centroid_point, 10).getInfo()
        except (EEException, HttpError):
            logging.error(f" Invalid centroid {centroid_point}")
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

    def _get_xy(self, building_footprint_gdf):
        try:
            building_footprint_gdf["centroid_geometry"] = building_footprint_gdf[
                "centroid_geometry"
            ].map(shapely.wkt.loads)

        except TypeError:
            pass

        building_footprint_gdf["x"] = building_footprint_gdf.centroid_geometry.map(
            lambda p: p.x
        )
        building_footprint_gdf["y"] = building_footprint_gdf.centroid_geometry.map(
            lambda p: p.y
        )
        return building_footprint_gdf

    def _replace_symbol(self, item):
        return str(item).replace(".", "_")

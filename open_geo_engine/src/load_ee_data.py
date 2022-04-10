from typing import Tuple, Sequence, Any
import datetime
from joblib import Parallel, delayed
import pandas as pd
import shapely
import ee
import geemap

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
            image_collection=config.LANDSAT_IMAGE_COLLECTION,
            image_band=config.LANDSAT_IMAGE_BAND,
            folder=config.BASE_FOLDER,
            image_folder=config.IMAGE_FOLDER,
            model_name=config.MODEL_NAME,
            place=config.PLACE,
        )

    def execute(self, save_images):
        building_footprint_gdf = pd.read_csv("local_data/kurdistan_flaring_points.csv")

        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_country)(building_footprint_gdf, save_images)
            for country in self.countries
        )

    def execute_for_country(self, building_footprint_gdf, save_images):
        print(f"Downloading {self.model_name} data for {self.place}")
        building_footprint_gdf = self._get_xy(building_footprint_gdf)
        building_footprints_satellite_list = []
        for lon, lat in zip(building_footprint_gdf.x, building_footprint_gdf.y):
            # Initialize the library.
            ee.Initialize()
            centroid_point = ee.Geometry.Point(lon, lat)
            s_datetime, e_datetime = self._generate_start_end_date()
            collection = (
                ee.ImageCollection(self.image_collection)
                .filterBounds(centroid_point)
                .select(self.image_band)
                .filterDate(s_datetime, e_datetime)
            )
            landsat_centroid_point = collection.getRegion(centroid_point, 10).getInfo()
            building_footprints_satellite_list.append(
                ee_array_to_df(landsat_centroid_point, self.image_band)
            )
            if save_images is True:
                self.save_images_to_drive(collection, s_datetime, e_datetime, lon, lat)
        return pd.concat(building_footprints_satellite_list)

    def save_images_to_drive(self, collection, s_datetime, e_datetime, lon, lat):
        s_date = s_datetime.date()
        e_date = e_datetime.date()
        lon_without_symbol = self._replace_symbol(lon)
        lat_without_symbol = self._replace_symbol(lat)
        geemap.ee_export_image_collection(
            collection,
            out_dir=f"{self.image_folder}/{self.model_name}_{s_date}_{e_date}_{lat_without_symbol}_{lon_without_symbol}",
        )

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

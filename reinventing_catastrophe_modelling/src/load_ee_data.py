from typing import Dict, Tuple, Sequence, Any
import datetime
from joblib import Parallel, delayed
import ee

import bootstrap  # noqa
from config.model_settings import DataConfig


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
        model_name: str,
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
            image_collection=config.LANDSAT_IMAGE_COLLECTION,
            image_band=config.LANDSAT_IMAGE_BAND,
            folder=config.BASE_FOLDER + config.LANDSAT_FOLDER,
            model_name=config.MODEL_NAME,
        )

    def execute(self):
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_country)(country) for country in self.countries
        )

    def execute_for_country(self, country: str):
        # Initialize the library.
        ee.Initialize()
        print(f"Executing data download for {country[0]}")
        dates = self.prepare_dates()
        for i in range(len(dates) - 1):
            s_date = dates[i]
            e_date = dates[i + 1]
            print(f"Downloading {model_name} data for dates: {s_date}-{e_date}")
            area = ee.Geometry.Rectangle(list(country[1]))
            collection = (
                ee.ImageCollection(self.image_collection)
                .select(self.image_band)
                .filterDate(s_date, e_date)
            )

            img = collection.mean()

            down_args = {
                "image": img,
                "region": area,
                "folder": f"{self.folder}",
                "description": f"{self.model_name}_{s_date}_{e_date}",
                "scale": 10,
            }
            task = ee.batch.Export.image.toDrive(**down_args)
            task.start()

    def prepare_dates(self) -> Tuple[datetime.date, datetime.date]:
        start, end = self._generate_start_end_date()
        date_list = self._date_range(start, end)
        return self._generate_dates(date_list)

    def _generate_start_end_date(self) -> Tuple[datetime.date, datetime.date]:
        start = datetime.date(self.year, self.mon_start, self.date_start)
        end = datetime.date(self.year_end, self.mon_end, self.date_end)
        return start, end

    def _date_range(self, start, end) -> Sequence[Any]:
        r = (end + datetime.timedelta(days=1) - start).days
        return [start + datetime.timedelta(days=i) for i in range(0, r, 7)]

    def _generate_dates(self, date_list) -> Sequence[str]:
        return [str(date) for date in date_list]

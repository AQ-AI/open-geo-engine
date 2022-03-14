from typing import Dict, Sequence, Any, Tuple
from joblib import Parallel, delayed

import osmnx as ox
import pandas as pd
import geopandas as gpd
from open_geo_engine.utils.utils import write_csv

from open_geo_engine.config.model_settings import DataConfig, OSMConfig


class GenerateBuildingCentroids:
    def __init__(
        self,
        countries: Sequence,
        place: str,
        tags: Dict[str, Any],
    ):
        self.countries = countries
        self.place = place
        self.tags = tags

    @classmethod
    def from_dataclass_config(
        cls,
        data_config: DataConfig,
        osm_config: OSMConfig,
    ) -> "GenerateBuildingCentroids":
        countries = []
        for country in data_config.COUNTRY_CODES:
            country_info = data_config.COUNTRY_BOUNDING_BOXES.get(country, "WO")
            countries.append(country_info)

        return cls(countries=countries, place=osm_config.PLACE, tags=osm_config.TAGS)

    def execute(self, list_of_points):
        print(f"Downloading {self.tags} for {self.place}")
        if list_of_points:
            building_footprint_gdfs = Parallel(
                n_jobs=-1, backend="multiprocessing", verbose=5
            )(
                delayed(self.execute_for_country)(i, lat_lon)
                for i, lat_lon in enumerate(list_of_points)
            )
        else:
            building_footprint_gdfs = Parallel(
                n_jobs=-1, backend="multiprocessing", verbose=5
            )(delayed(self.execute_for_country)(country) for country in self.countries)
        return pd.concat(building_footprint_gdfs)

    def execute_for_country(self, i: int, lat_lon: str):
        ox.config(log_console=False, use_cache=True)
        return self.get_representative_building_point(i, lat_lon)

    def _get_boundaries_from_place(self) -> gpd.GeoDataFrame:
        return ox.geometries.geometries_from_place(self.place, self.tags)

    def get_representative_building_point(self, i, lat_lon) -> gpd.GeoDataFrame:
        if lat_lon:
            building_footprints = ox.geometries.geometries_from_point(
                lat_lon, self.tags, 1000
            )
            if i == 0:
                building_footprints.to_csv(
                    "local_data/residential_buildings_flare_1km.csv",
                    mode='w',
                    index=False,
                    header=True,
                )
            else:
                building_footprints.to_csv(
                    "local_data/residential_buildings_flare_1km.csv",
                    mode='a',
                    index=False,
                    header=False,
                )

        else:
            building_footprints = self._get_boundaries_from_place()

            building_footprints[
                "centroid_geometry"
            ] = building_footprints.representative_point()
        return building_footprints

from typing import Dict, Sequence, Any
from joblib import Parallel, delayed

import osmnx as ox
import pandas as pd
import geopandas as gpd

from open_geo_engine.config.model_settings import DataConfig, OSMConfig


class GenerateBuildingCentroids:
    def __init__(self, countries: Sequence, place: str, tags: Dict[str, Any]):
        self.countries = countries
        self.place = place
        self.tags = tags

    @classmethod
    def from_dataclass_config(
        cls, data_config: DataConfig, osm_config: OSMConfig
    ) -> "GenerateBuildingCentroids":
        countries = []
        for country in data_config.COUNTRY_CODES:
            country_info = data_config.COUNTRY_BOUNDING_BOXES.get(country, "WO")
            countries.append(country_info)

        return cls(countries=countries, place=osm_config.PLACE, tags=osm_config.TAGS)

    def execute(self):
        building_footprint_gdfs = Parallel(
            n_jobs=-1, backend="multiprocessing", verbose=5
        )(delayed(self.execute_for_country)(country) for country in self.countries)
        return pd.concat(building_footprint_gdfs)

    def execute_for_country(self, country: str):
        return self.get_representative_building_point()

    def _get_boundaries_from_place(self) -> gpd.GeoDataFrame:
        return ox.geometries.geometries_from_place(self.place, self.tags)

    def get_representative_building_point(self) -> gpd.GeoDataFrame:
        building_footprints = self._get_boundaries_from_place()
        building_footprints[
            "centroid_geometry"
        ] = building_footprints.representative_point()
        return building_footprints

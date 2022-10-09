import os
from joblib import Parallel, delayed
from typing import Sequence

import pandas as pd
import geopandas as gpd
from open_geo_engine.utils.utils import read_csv, write_csv
from open_geo_engine.config.model_settings import DataConfig


class PredictionstoGeoJSON:
    def __init__(self, countries: Sequence):
        self.countries = countries

    @classmethod
    def from_dataclass_config(cls, config: DataConfig) -> "PredictionstoGeoJSON":
        countries = []
        for country in config.COUNTRY_CODES:
            country_info = config.COUNTRY_BOUNDING_BOXES.get(country, "WO")
            countries.append(country_info)
        return cls(
            countries=countries,
        )

    def execute(self, csv_folder, geojson_folder):
        """Iterate through each predicted csv and convert to a format viewable in kepler
        Arguments
        ---------
            csv_folder : str
                      A folder containing the csv predictions
            geojson_folder: str
                      a folder to save processed geojsons
        Returns
        ---------
            appended geojson with predictions for all time
        """

        gdf_list = []
        gdf_list.append(Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_csv)(csv_folder, geojson_folder, csv_filepath)
            for csv_filepath in os.listdir(csv_folder)
        )
        )
        timeseries_clipped_gdf = pd.concat(gdf_list[0])
        timeseries_clipped_gdf.to_file(f"{geojson_folder}/pm25_merged.geojson")
    
    def execute_for_csv(self, csv_folder, geojson_folder, csv_file):
      """execute the script for each csv"""
      df = read_csv(f"{csv_folder}/{csv_file}")
      gdf = gpd.GeoDataFrame(
          df, geometry=gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
      )
      gdf_clipped = self._clip_by_bounding_box(gdf)
      gdf_clipped_datetime = self._convert_week_str_to_datetime(gdf_clipped)
      gdf_clipped_datetime.to_file(f"{geojson_folder}/{csv_file[:-4]}_MN.geojson")
      return gdf_clipped


    def _clip_by_bounding_box(self, gdf):
        """Clip gdf by the list containing bounding boxes"""
        # cropped_gdf = [gdf.cx[bbox[1][1] : bbox[1][3], bbox[1][0] : bbox[1][2]] for bbox in self.countries]
        clipped_gdf_list = []
        for bbox in self.countries:
          gdf["country"] = str(bbox[0])
          clipped_gdf_list.append(gdf.cx[bbox[1][0] : bbox[1][2], bbox[1][1]: bbox[1][3]])
        return pd.concat(clipped_gdf_list, ignore_index=True)

    def _convert_week_str_to_datetime(self, gdf):
      """Convert week string column to separate datetime column"""
      gdf["week_beginning"] = gdf['week'].apply(lambda week_str: week_str.split("_")[0])
      gdf["week_beginning"] = pd.to_datetime(gdf["week_beginning"])
      return gdf


if __name__ == "__main__":
    csv_folder = "local_data/Predicted_Data/csv"
    geojson_folder = "local_data/Predicted_Data/geojson"
    config = DataConfig()

    prediction_to_geojson = PredictionstoGeoJSON.from_dataclass_config(config)
    prediction_to_geojson.execute(csv_folder, geojson_folder)

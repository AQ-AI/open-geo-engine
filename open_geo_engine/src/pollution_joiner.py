import os
from functools import reduce

from os.path import isfile, join
from datetime import datetime
import pandas as pd
from sklearn.impute import SimpleImputer

from joblib import Parallel, delayed
from open_geo_engine.utils.utils import read_csv, write_csv
from open_geo_engine.config.model_settings import PollutionJoinerConfig


class PollutionJoiner:
    def __init__(self, location_file, satellite_dir, pollution_dir, time_aggregations, joined_dir):
        self.location_file = location_file
        self.satellite_dir = satellite_dir
        self.pollution_dir = pollution_dir
        self.time_aggregations = time_aggregations
        self.joined_dir = joined_dir

    @classmethod
    def from_dataclass_config(cls, config: PollutionJoinerConfig) -> "PollutionJoiner":
        return cls(
            location_file=config.location_file,
            satellite_dir=config.satellite_dir,
            pollution_dir=config.pollution_dir,
            time_aggregations=config.time_aggregations,
            joined_dir=config.joined_dir,
        )

    def execute(self):
       """
        Executes code to join pollution (from static files) and satellite imagery to form training data for machine learning.

        Arguments
        ---------
            location_file : str
                file path on where the locations of the pollution sensors
            satellite_dir : str
                folder locations of all the satellite data csvs extracted using `LoadEEData`
            pollution_dir : str
                The folder locations of the pollution files
            time_aggregations : list
                list of the temporal aggregations of the satellite images ("date" or "month:)
            joined_dir : tuple
                the folder location
        """
        all_satellite_list = []
        for time_aggregation in self.time_aggregations:
            satellite_list = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_satellite_files)(satellite_filepath, time_aggregation)
                for satellite_filepath in os.listdir(f"{self.satellite_dir}/{time_aggregation}")
            )
            all_satellite_list.extend(satellite_list)
        
        all_pollution_satellite_list = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_pollution_file)(filepath, all_satellite_list)
            for filepath in os.listdir(self.pollution_dir)
        )
        train_df = pd.concat(all_pollution_satellite_list, ignore_index=True)
        
        write_csv(
            train_df,
            f"{self.joined_dir}/train_nulls.csv",
        )

    def execute_for_satellite_files(self, satellite_filepath, time_aggregation):
        return read_csv(f"{self.satellite_dir}/{time_aggregation}/{satellite_filepath}")

    def execute_for_pollution_file(self, pollution_filepath, satellite_list):
        pollution_df = read_csv(f"{self.pollution_dir}/{pollution_filepath}")
        pollution_df = self._associate_sensor_location(pollution_df, pollution_filepath)
        joined_df = self.join_datewise_dfs(pollution_df, satellite_list, pollution_filepath)

        return joined_df

    def join_datewise_dfs(self, pollution_df, satellite_list, pollution_filepath):
        df_list = []
        pollution_df["longitude"] = pollution_df["longitude"].astype(float)
        pollution_df["latitude"] = pollution_df["latitude"].astype(float)
        for i, sat_df in enumerate(satellite_list):
            if i == 0 and "month" in sat_df.columns:
                pollution_df["month"] = pollution_df.apply(
                    lambda row: self._format_pollution_month_year(row["timestamp"]), axis=1
                )
                pollution_sat_df = self.merge_pollution_sat_df(sat_df, pollution_df, "month")

            if i == 0 and "datetime" in sat_df.columns:
                pollution_df["datetime"] = pollution_df.apply(
                    lambda row: self._format_pollution_date(row["timestamp"]), axis=1
                )
                pollution_sat_df = self.merge_pollution_sat_df(sat_df, pollution_df, "datetime")
            if i > 0:
                pollution_df = pollution_sat_df
                if "month" in sat_df.columns:
                    pollution_df["month"] = pollution_df.apply(
                        lambda row: self._format_pollution_month_year(row["timestamp"]), axis=1
                    )
                    pollution_sat_df = self.merge_pollution_sat_df(sat_df, pollution_df, "month")

                if "datetime" in sat_df.columns:
                    pollution_sat_df = self.merge_pollution_sat_df(sat_df, pollution_df, "datetime")
        
        df_list.append(pollution_sat_df)
        return pd.concat(df_list, axis=1)

    def merge_pollution_sat_df(self, sat_df, pollution_df, time_col):
        pollution_df = self._create_unique_id(pollution_df, time_col)
        sat_df = self._create_unique_id(sat_df, time_col)
        cols_to_use = list(sat_df.columns.difference(pollution_df.columns)) + [
            f"unique_id_{time_col}",
        ]

        return pollution_df.merge(
            sat_df[cols_to_use],
            how="left",
            on=f"unique_id_{time_col}",
        )

    def _format_pollution_date(self, col):
        return datetime.strptime(col, "%Y-%m-%d %H").date()

    def _format_pollution_month_year(self, col):
        return datetime.strptime(col, "%Y-%m-%d %H").strftime("%Y-%m")

    def _associate_sensor_location(self, pollution_df, pollution_filepath):
        sensor_locations_df = read_csv(self.location_file)
        sensor_filtered_df = self._filter_sensors(sensor_locations_df, pollution_filepath)
        pollution_df = pollution_df.apply(
            lambda row: self._format_lat_lon(row, sensor_filtered_df), axis=1
        )

        return pollution_df

    def _create_unique_id(self, df, time_col):
        df[f'unique_id_{time_col}'] = ["_".join(dict.fromkeys(str(k) for k in v if pd.notnull(k))) for v in
                  df[[time_col, "latitude", "longitude"]].values]
        return df 

    def _filter_sensors(self, sensor_locations_df, pollution_filepath):
        filtered_sensors = sensor_locations_df[
            sensor_locations_df["device_name"].str.contains(pollution_filepath)
        ]
        return filtered_sensors

    def _format_lat_lon(self, row, sensor_filtered_df):
        row["longitude"] = format(float(sensor_filtered_df["x"]), ".4f")
        row["latitude"] = format(float(sensor_filtered_df["y"]), ".4f")
        return row

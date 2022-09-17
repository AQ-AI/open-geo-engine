import os
from os.path import isfile, join
from datetime import datetime
import pandas as pd
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
        non_csv_filetypes = [
            ".DS_Store",
        ]
        pollution_filepaths = [
            filename
            for filename in os.listdir(self.pollution_dir)
            if os.path.isfile(os.path.join(self.pollution_dir, filename))
            and not any(filetype in filename for filetype in non_csv_filetypes)
        ]
        pollution_satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_pollution_file)(filepath)
                for filepath in pollution_filepaths
            )
        )
        write_csv(
            pollution_satellite_df,
            f"{self.joined_dir}/train_df.csv",
        )

    def execute_for_pollution_file(self, pollution_filepath):
        print(pollution_filepath)
        pollution_satellite_list = []
        for time_aggregation in self.time_aggregations:
            pollution_satellite_list.append(
                pd.concat(
                    Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                        delayed(self.execute_for_pollution_satellite_files)(
                            satellite_filepath, pollution_filepath, time_aggregation
                        )
                        for satellite_filepath in os.listdir(
                            f"{self.satellite_dir}/{time_aggregation}"
                        )
                    )
                )
            )
        return pd.concat(pollution_satellite_list)

    def execute_for_pollution_satellite_files(
        self, satellite_filepath, pollution_filepath, time_aggregation
    ):
        satellite_df = read_csv(f"{self.satellite_dir}/{time_aggregation}/{satellite_filepath}")
        pollution_df = read_csv(f"{self.pollution_dir}/{pollution_filepath}")
        pollution_df["datetime"] = pollution_df.apply(
            lambda row: self._format_pollution_date(row["timestamp"]), axis=1
        )
        pollution_df = self._associate_sensor_location(pollution_df, pollution_filepath)
        joined_df = self.join_datewise_dfs(satellite_filepath, pollution_df, satellite_df)
        return joined_df

    def join_datewise_dfs(self, satellite_filepath, pollution_df, satellite_df):
        pollution_df["datetime"] = pd.to_datetime(pollution_df["datetime"], errors="coerce")
        satellite_df["datetime"] = pd.to_datetime(satellite_df["datetime"], errors="coerce")
        if "month" in satellite_filepath:
            satellite_df["month"] = satellite_df.datetime.month()
            pollution_df["month"] = pollution_df.datetime.month()
            joined_df = pollution_df.merge(satellite_df,
                how="inner",
                on=["month", "latitude", "longitude"],
            )
        else:
            joined_df = pollution_df.merge(satellite_df,
                how="inner",
                on=["datetime", "latitude", "longitude"],
            )
        return joined_df

    def _format_pollution_date(self, col):
        return datetime.strptime(col, "%Y-%m-%d %H").date()

    def _associate_sensor_location(self, pollution_df, pollution_filepath):
        sensor_locations_df = read_csv(self.location_file)
        pollution_df["longitude"] = self._filter_longitude(sensor_locations_df, pollution_filepath)
        pollution_df["latitude"] = self._filter_latitude(sensor_locations_df, pollution_filepath)

        return pollution_df

    def _filter_longitude(self, sensor_locations_df, pollution_filepath):
        filtered_sensors = sensor_locations_df[
            sensor_locations_df["device_name"].str.contains(pollution_filepath)
        ]
        return round(float(filtered_sensors["x"]), 4)

    def _filter_latitude(self, sensor_locations_df, pollution_filepath):
        filtered_sensors = sensor_locations_df[
            sensor_locations_df["device_name"].str.contains(pollution_filepath)
        ]
        return round(float(filtered_sensors["y"]), 4)

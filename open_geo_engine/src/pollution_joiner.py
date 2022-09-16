import os
from os.path import isfile, join
from datetime import datetime
import pandas as pd
from joblib import Parallel, delayed
from open_geo_engine.utils.utils import read_csv, write_csv
from open_geo_engine.config.model_settings import PollutionDataConfig

class PollutionJoiner:
    def __init__(self, location_file, satellite_dir, pollution_dir):
        self.location_file = location_file
        self.satellite_dir = satellite_dir
        self.pollution_dir = pollution_dir

    @classmethod
    def from_dataclass_config(cls, config: PollutionDataConfig) -> "PollutionJoiner":
        return cls(
            location_file=config.location_file,
            satellite_dir=config.satellite_dir,
            pollution_dir=config.pollution_dir,
        )
    
    def execute(
        self
    ):
        non_csv_filetypes = [
            ".DS_Store",
        ]
        pollution_filepaths = [
            filename
            for filename in os.listdir(self.pollution_dir)
            if os.path.isfile(os.path.join(self.pollution_dir, filename))
            and not any(filetype in filename for filetype in non_csv_filetypes)
        ]
        pollution_satellite_dfs = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_pollution_file)(filepath)
                for filepath in pollution_filepaths
            )
        )
        write_csv(
            pollution_satellite_dfs,
            f"{self.described_dir}/{filepath[:-4]}.csv",
        )


    def execute_for_pollution_file(self, pollution_filepath):
        pollution_satellite_dfs = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_pollution_satellite_files)(satellite_filepath, pollution_filepath)
                for satellite_filepath in self.satellite_dir
            )
        )

    def execute_for_pollution_satellite_files(satellite_filepath, pollution_filepath):
        satellite_df = read_csv(f"{self.satellite_dir}/{satellite_filepath}")
        pollution_df = read_csv(f"{self.pollution_dir}/{pollution_filepath}")
        pollution_df["datetime"] = pollution_df.apply(lambda row: self._format_pollution_date(row["timestamp"]), axis=1)
        pollution_df = self._associate_sensor_location(pollution_df, pollution_filepath)

    def join_datewise_dfs(self, pollution_df, satellite_df):
        res = pd.merge(pollution_df.assign(grouper=df1['date_1'].dt.to_period('M')),
               df2.assign(grouper=df2['date_2'].dt.to_period('M')),
               how='left', on='grouper')
            pollution_df.join(satellite_df, on=["datetime", "longitude", "latitude"], axis=1)

    def _format_pollution_date(self, col):
        return datetime.strptime(col, "YYYY-MM-DD HH").date()

    def _associate_sensor_location(self, pollution_df, pollution_filepath):
        sensor_locations_df = read_csv(self.location_file) 
        pollution_df["longitude"] = sensor_locations_df.apply(lambda row: [row["x"] if row["device_name"] == pollution_filepath else pass], axis=1)
        pollution_df["latitude"] = sensor_locations_df.apply(lambda row: [row["y"] if row["device_name"] == pollution_filepath else pass], axis=1)

        return pollution_df
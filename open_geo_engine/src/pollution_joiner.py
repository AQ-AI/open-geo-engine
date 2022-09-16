import os
from os.path import isfile, join

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
        pollution_daily_dfs = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_file)(filepath)
                for filepath in pollution_filepaths
            )
        )
        write_csv(
            metric_over_time_df,
            f"{self.described_dir}/{filepath[:-4]}.csv",
        )


    def execute_for_file(self,):
        pollution_satellite_dfs = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.join_daily_satellite)(satellite_filepath, pollution_filepath)
                for filepath in self.satellite_dir
            )
        )

    def join_datewise_csvs(satellite_filepath, pollution_filepath):
        satellite_df = read_csv(f"{self.satellite_dir}/{satellite_filepath}")
        pollution_df = read_csv(f"{self.pollution_dir}/{pollution_filepath}")

    
    def _formate_pollution_date(self, satellite_filepath, pollution_filepath):
        ...
    def _associate_sensor_location() 
        ...
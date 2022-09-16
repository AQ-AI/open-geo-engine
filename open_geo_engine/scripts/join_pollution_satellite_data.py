import os
from os.path import isfile, join

import pandas as pd
from joblib import Parallel, delayed
from open_geo_engine.utils.utils import read_csv, write_csv
from open_geo_engine.config.model_settings import PollutionDataConfig

class Pointoiner:
    def __init__(self, ):
        self.location_filepath = location_filepath
def join_datewise_csvs(filepath, satellite_filepaths):

    pollution_df = read_csv(f"/home/ubuntu/unicef_work/open-geo-engine/local_data/pollution_data/{filepath}")
    pollution_daily_dfs = pd.concat(
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(_join_daily_satellite)(satellite_filepath, pollution_df)
            for filepath in satellite_filepaths
        )
    )

def _join_daily_satellite(satellite_filepaths, pollution_df):
    pollution_df.join()
    
if __name__ == "__main__":
    non_flaring_filetypes = [
        ".DS_Store",
    ]
    pollution_filepaths = [
        filename
        for filename in os.listdir("ee_data")
        if isfile(join("ee_data", filename))
        and not any(filetype in filename for filetype in non_flaring_filetypes)
    ]
    pollution_dfs = pd.concat(
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(pollution_data_dir)(filepath)
            for filepath in methane_filepaths
        )
    )
    write_csv(
        methane_by_date_df.drop_duplicates(),
        "summarised_data/kurdistan_processed_data/methane_concentrations.csv",
    )

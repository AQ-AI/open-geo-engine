from datetime import datetime
import os
import pandas as pd
from joblib import Parallel, delayed

from open_geo_engine.utils.utils import read_csv, write_csv
from open_geo_engine.config.model_settings import SatelliteTemporalAggregatorConfig

class SatelliteTemporalAggregator:
    def __init__(
        self,
        processed_target_df: pd.DataFrame,
        described_dir: str,
        time_aggregation: str,
    ):
        self.processed_target_df = processed_target_df
        self.described_dir = described_dir
        self.time_aggregation = time_aggregation

    @classmethod
    def from_dataclass_config(cls, config: SatelliteTemporalAggregatorConfig) -> "SatelliteTemporalAggregator":
        return cls(
            processed_target_df=config.processed_target_df,
            described_dir=config.described_dir,
            time_aggregation=config.time_aggregation,)

    def execute(
        self
    ):
        non_csv_filetypes = [
            ".DS_Store",
        ]
        satellite_df_filepaths = [
            filename
            for filename in os.listdir(self.processed_target_df)
            if os.path.isfile(os.path.join(self.processed_target_df, filename))
            and not any(filetype in filename for filetype in non_csv_filetypes)
        ]
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_file)(filepath)
            for filepath in satellite_df_filepaths
        )

    def execute_for_file(self, filepath):
        satellite_df = read_csv(f"{self.processed_target_df}/{filepath}")
        metric_over_time_df = self.calculate_values_over_time_aggregation(
            satellite_df
        )
        write_csv(
            metric_over_time_df,
            f"{self.described_dir}/{self.time_aggregation}_{filepath[:-4]}.csv",
        )

    def calculate_values_over_time_aggregation(self, satellite_df):
        if self.time_aggregation == "date":
            satellite_df["datetime"] = pd.to_datetime(
                satellite_df["datetime"]
            ).dt.to_period("D")

            satellite_grpby = (
                satellite_df.groupby(["datetime", "latitude", "longitude"])
                .mean()
                .reset_index()
            )
            satellite_grpby["timestamp"] = satellite_grpby.apply(
                lambda row: self._get_dately_timestamp(row), axis=1
            )

        else:
            satellite_df["datetime"] = pd.to_datetime(
                satellite_df["datetime"]
            ).dt.to_period("M")
            satellite_grpby = (
                satellite_df.groupby(["datetime", "latitude", "longitude"])
                .mean()
                .reset_index()
            )
            satellite_grpby["timestamp"] = satellite_grpby.apply(
                lambda row: self._get_monthly_timestamp(row), axis=1
            )

        return satellite_grpby

    def _get_year_from_files(self, row):
        """Filter filenames based on IDs and publication dates"""
        return str(row.year)

    def _replace_symbol(row, item):
        return str(item).replace(".", "_")

    def _get_dately_timestamp(self, row):
        return datetime.strptime(str(row.datetime), "%Y-%m-%d").timestamp()

    def _get_monthly_timestamp(self, row):
        return datetime.strptime(str(row.datetime), "%Y-%m").timestamp()
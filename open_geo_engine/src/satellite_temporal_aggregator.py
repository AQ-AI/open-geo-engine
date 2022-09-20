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
        time_aggregations: str,
    ):
        self.processed_target_df = processed_target_df
        self.described_dir = described_dir
        self.time_aggregations = time_aggregations

    @classmethod
    def from_dataclass_config(
        cls, config: SatelliteTemporalAggregatorConfig
    ) -> "SatelliteTemporalAggregator":
        return cls(
            processed_target_df=config.processed_target_df,
            described_dir=config.described_dir,
            time_aggregations=config.time_aggregations,
        )

    def execute(self):

        satellite_list = []
        for time_aggregation in self.time_aggregations:
            metric_over_time_df = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(self.execute_for_file)(filepath, time_aggregation)
                for filepath in os.listdir((f"{self.processed_target_df}/{time_aggregation}"))
            )
            # print(type(metric_over_time_df), metric_over_time_df)
            # write_csv(
            #     metric_over_time_df,
            #     f"{self.described_dir}/{time_aggregation}/{metric_over_time_df[:-4]}.csv",
            # )

    def execute_for_file(self, filepath, time_aggregation):
        satellite_df = read_csv(f"{self.processed_target_df}/{time_aggregation}/{filepath}")
        satellite_df = satellite_df.apply(lambda row: self._round_lat_lon(row), axis=1)
        metric_over_time_df = self.calculate_values_over_time_aggregation(
            satellite_df, time_aggregation
        )
        write_csv(
            metric_over_time_df,
            f"{self.described_dir}/{time_aggregation}/{filepath[:-4]}.csv",
        )
        # return metric_over_time_df

    def calculate_values_over_time_aggregation(self, satellite_df, time_aggregation):
        if time_aggregation == "date":
            satellite_df["datetime"] = pd.to_datetime(satellite_df["datetime"]).dt.to_period("D")

            satellite_grpby = (
                satellite_df.groupby(["datetime", "latitude", "longitude"]).mean().reset_index()
            )
        else:
            satellite_df["month"] = pd.to_datetime(satellite_df["datetime"]).dt.to_period("M")
            satellite_grpby = (
                satellite_df.groupby(["month", "latitude", "longitude"]).mean().reset_index()
            )
        return satellite_grpby

    def _round_lat_lon(self, row):
        row["longitude"] = format(float(row["longitude"]), ".4f")
        row["latitude"] = format(float(row["latitude"]), ".4f")
        return row

    def _get_year_from_files(self, row):
        """Filter filenames based on IDs and publication dates"""
        return str(row.year)

    def _replace_symbol(row, item):
        return str(item).replace(".", "_")

    def _get_dately_timestamp(self, row):
        return datetime.strptime(str(row.datetime), "%Y-%m-%d").timestamp()

    def _get_monthly_timestamp(self, row):
        return datetime.strptime(str(row.datetime), "%Y-%m").timestamp()

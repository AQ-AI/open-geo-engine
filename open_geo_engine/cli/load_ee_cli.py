import click
from click_option_group import OptionGroup

from utils.utils import parametrized
from config.model_settings import DataConfig


@parametrized
def load_data_options(fn, countries_option: bool = True):
    """
    countries_option: bool = True
        Whether to provide the option to specify countries or not
    """
    load_data_config = OptionGroup(
        "Options for loading data",
        help="Allows loading of data from custom provided country codes",
    )
    # country_codes_ = load_data_config.option(
    #     "-c",
    #     "--country_codes",
    #     default=DataConfig.COUNTRY_CODES,
    #     type=click.STRING,
    #     help="Countries to load data from",
    # )
    year_ = load_data_config.option(
        "-y",
        "--year",
        default=DataConfig.YEAR,
        type=click.INT,
        help="Start year to load data from",
    )
    year_end_ = load_data_config.option(
        "-e",
        "--end-year",
        default=DataConfig.YEAR_END,
        type=click.INT,
        help="End year to load data from",
    )
    wrapped_func = year_(year_end_(fn))

    # if countries_option:
    #     wrapped_func = country_codes_(wrapped_func)

    return wrapped_func

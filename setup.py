# setup.py
import os

from setuptools import setup

PACKAGE_NAME = "open_geo_engine"

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./"))


setup(
    name=PACKAGE_NAME,
    entry_points={
        "console_scripts": [
            "open-geo-engine = open_geo_engine.main:cli",
        ]
    },
)

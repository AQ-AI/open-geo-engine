# setup.py
import os

from setuptools import setup

PACKAGE_NAME = "reinventing_catastrophe_modelling"

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./"))


setup(
    name=PACKAGE_NAME,
    entry_points={
        "console_scripts": [
            "reinventing-catastrophe-modelling = reinventing_catastrophe_modelling.main:cli",
        ]
    },
)

# -*- coding: utf-8 -*-
from setuptools import setup

packages = [
    "reinventing_catastrophe_modelling",
    "reinventing_catastrophe_modelling.cli",
    "reinventing_catastrophe_modelling.config",
    "reinventing_catastrophe_modelling.src",
    "reinventing_catastrophe_modelling.utils",
]

package_data = {"": ["*"]}

install_requires = [
    "Django>=3.2.6,<4.0.0",
    "PyYAML>=5.4.1,<6.0.0",
    "bandit>=1.7.0,<2.0.0",
    "black>=21.7b0,<22.0",
    "catboost>=0.26.1,<0.27.0",
    "click-config-file>=0.6.0,<0.7.0",
    "click-option-group>=0.5.3,<0.6.0",
    "earthengine-api>=0.1.280,<0.2.0",
    "esda>=2.4.1,<3.0.0",
    "geopandas>=0.9.0,<0.10.0",
    "google-streetview>=1.2.9,<2.0.0",
    "google>=3.0.0,<4.0.0",
    "hydra-core>=1.1.1,<2.0.0",
    "joblib>=1.0.1,<2.0.0",
    "libpysal>=4.5.1,<5.0.0",
    "loguru>=0.5.3,<0.6.0",
    "mlflow>=1.20.1,<2.0.0",
    "osmnx>=1.1.1,<2.0.0",
    "pydantic>=1.8.2,<2.0.0",
    "pylama>=7.7.1,<8.0.0",
    "setuptools>=57.4.0,<58.0.0",
    "sklearn>=0.0,<0.1",
]

setup_kwargs = {
    "name": "reinventing-catastrophe-modelling",
    "version": "0.1.2",
    "entry_points": {
        "console_scripts": [
            "reinventing-catastrophe-modelling = reinventing_catastrophe_modelling.__main__:main"
        ]
    },
    "description": "",
    "long_description": None,
    "author": "ChristinaLast",
    "author_email": "christina.last@outlook.com",
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "python_requires": ">=3.8,<4.0",
}


setup(**setup_kwargs)

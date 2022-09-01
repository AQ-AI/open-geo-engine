from setuptools import find_packages, setup

setup(
    name="open-geo-engine",
    packages=find_packages(),
    version="0.1.0",
    install_requires=[
        "Click",
        "geopandas",
    ],
    entry_points="""
        [console_scripts]
        open-geo-engine=main:cli
    """,
    description="""
    """,
    author="AQAI",
    license="",
)

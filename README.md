# :ocean: Reinventing catastrophe modelling

## Introduction
This library aims to develop an approach to catastrophe modelling that combines satellite imagery analysis and geospatial data science.

## :nerd_face: Why might you want to use this
Catastrophe modelling relates natural disasters to future pay-outs. With the advent of climate change, however, nature is no longer behaving like it used to, and past data is becoming less predictive of future events. This project responds to this call for action to enable catastrophe modelling by bringing near-time satellite data measuring environmental and social phenomena alongside urban geospatial data. This project enables users to combine these disparate data sources together for timeseries analysis.

### Workflow
1. Retrieves building footprints and calculates their centroids using Open Street Map.
2. Takes in centroid list from Open Street Map data, and generates a timeseries of satellite band data in `dataframe` format.
3. From the extracted latitude and longitude values, requests the google streetview image via API and downloads to a local folder (saving the request link amd metadata in `dataframe` format).

## :hammer_and_wrench: How to use this repository
### :wrench: Setup
#### :motorway: Set up Google Streetview API Key
1. Follow the instructions [here](https://developers.google.com/maps/documentation/streetview/get-api-key#get-key) to set up an API key. Prerequisites are:
- Google account
- Access to the Google Cloud Platform
2. Enable Billing using these [instructions](https://cloud.google.com/billing/docs/how-to/modify-project) in the Google Cloud Project you activated the streetview API in.

## :package: Requirements
- Python > 3.8. If you have not yet installed python/a python version > 3.8 then see these [instructions](https://blog.jayway.com/2019/12/28/pyenv-poetry-saviours-in-the-python-chaos/) to complete an installation.
- This project uses `poetry` for package management. See [here](https://python-poetry.org/docs/) for installation instructions

### :hammer: Local Setup
1. Clone this repository

```
git clone https://github.com/ChristinaLast/reinventing-catastrophe-modelling.git
```

2. Fill in the required environmental variables in `.env.sample` and move to `.env`
3. Export the environmental variables using

```
source .env
```

4. Initialise the poetry environment using `poetry init`, ([docs](https://python-poetry.org/docs/basic-usage/#initialising-a-pre-existing-project) for installation instructions)

```
$ cd <local-path>reinventing-catastrophe-modelling
$ poetry init
$ poetry shell
```

5. Build the CLI locally using:

```
python setup.py develop
```

6. To see the functions available in the CLI
```
reinventing-catastrophe-modelling --help
```

7. Test the full pipeline works with the default settings

```
reinventing-catastrophe-modelling run_pipeline
```

8. Use the example notebooks to visualise the results of your analysis.
To create a kernel from your virtual environment:
`python -m ipykernel install --user --name venv --display-name "Revinventing Catastrophe Modelling Kernel"`

Then launch your jupyter notebook using the command:
`jupyter notebook`

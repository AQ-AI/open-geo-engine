PROJECT_NAME := open-geo-engine
PROJECT_VERSION := $(shell python setup.py --version)

SHELL := /bin/bash
BOLD := \033[1m
DIM := \033[2m
RESET := \033[0m

help:
	@echo "run - use this command to find help on running commands"
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "dist - package"
	@echo "install - install the package to the active Python's site-packages"
	@echo "lint-pylama - test linting againt pylama"
	@echo "lint-black - test linting againt black"
	@echo "bandit - analyze code for security issues"



.PHONY: all, install, uninstall, doc, dist

all: uninstall install clean

install:
	@echo -e "$(BOLD)installing $(PROJECT_NAME) $(PROJECT_VERSION)$(RESET)"
	@echo -e -n "$(DIM)"
	@pip install -e  .
	@echo -e -n "$(RESET)"

uninstall:
	@echo -e "$(BOLD)uninstalling '$(PROJECT_NAME)'$(RESET)"
	-@pip uninstall -y $(PROJECT_NAME) 2> /dev/null

doc:
	@echo -e "$(BOLD)building documentation for $(PROEJECT_NAME) $(PROJECT_VERSION)$(RESET)"
	@echo -e -n "$(DIM)"
	@cd docs && $(MAKE) html
	@echo -e -n "$(RESET)"

dist:
	@echo -e "$(BOLD)packaging $(PROEJECT_NAME) $(PROJECT_VERSION)$(RESET)"
	@echo -e -n "$(DIM)"
	@python setup.py sdist --dist-dir=dist
	@echo -e -n "$(RESET)"


.PHONY: clean, clean-pyc, clean-build

clean: clean-pyc clean-build

clean-pyc:
	@echo -e "Cleaning python file artefacts"
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '*~' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -fr {} +

clean-build:
	@echo -e "Removing build.."
	@rm -fr build/
	@echo -e "Removing dist.."
	@rm -fr dist/
	@echo -e "Removing eggs.."
	@rm -fr .eggs/
	@find . -name '*.egg-info' -exec rm -fr {} +
	@find . -name '*.egg' -exec rm -rf {} +


.PHONY: lint, lint-black, lint-pylama, isort

lint: lint-pylama lint-black isort

lint-black:
	@echo -e "linting with black.."
	@poetry run black . --line-length=100 --check --exclude /.venv

lint-pylama:
	@echo -e "linting with pylama.."
	@poetry run pylama  --skip .venv/*

isort:
	@echo -e "Sorting all imports.."
	@poetry run isort . --settings-path=setup.cfg

.PHONY: bandit

bandit:
	@echo -e "analysing code for security issues.."
	@poetry run bandit -r open-geo-engine -ll -ii

test:
	pytest -vv -s -vv tests/ --disable-pytest-warnings

test-db:
	pytest -vv -s -vv tests/ --disable-pytest-warnings

test-cov:
	pytest --cov=open_geo_engine tests/ -vv --disable-pytest-warnings

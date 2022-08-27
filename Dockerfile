FROM ubuntu:20.04

RUN DEBIAN_FRONTEND="noninteractive" TZ="America/New_York" 

RUN apt-get update && apt-get install -y python3.10 python3-pip curl


RUN apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev openssl


RUN curl https://pyenv.run | bash

# Use Python 3 for `python`, `pip`
# RUN    update-alternatives --install /usr/bin/python  python  /usr/bin/python3 1 \
#    && update-alternatives --install /usr/bin/pip     pip     /usr/bin/pip3    1


# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python3 -
ENV PATH "$PATH:/root/.local/bin/"

# Install Poetry packages (maybe remove the poetry.lock line if you don't want/have a lock file)
COPY pyproject.toml* ./
COPY poetry.lock* ./
RUN poetry init --no-interaction; (exit 0) # Does nothing if pyproject.toml exists
RUN poetry install --no-interaction

# Provide a known path for the virtual environment by creating a symlink
RUN ln -s $(poetry env info --path) /var/my-venv

# Clean up project files. You can add them with a Docker mount later.
RUN rm pyproject.toml poetry.lock

# Hide virtual env prompt
ENV VIRTUAL_ENV_DISABLE_PROMPT 1

# Start virtual env when bash starts
RUN echo 'source /var/my-venv/bin/activate' >> ~/.bashrc

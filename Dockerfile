FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND="noninteractive" TZ="America/New_York" apt-get install -y tzdata


RUN apt-get install -y python3.10 python3-pip curl


RUN apt-get install -y make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
    libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev openssl git


ENV HOME="/root"
WORKDIR ${HOME}
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

ENV PYTHON_VERSION=3.10.5
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}

# Use Python 3 for `python`, `pip`
# RUN    update-alternatives --install /usr/bin/python  python  /usr/bin/python3 1 \
#    && update-alternatives --install /usr/bin/pip     pip     /usr/bin/pip3    1
# install the gcloud
RUN sudo apt-get install apt-transport-https ca-certificates gnupg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN sudo apt-get update && sudo apt-get install google-cloud-cli


# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
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

FROM python:3.11-alpine

# Installs the local project
WORKDIR /pulse-telemetry
COPY ./src /pulse-telemetry/src
COPY pyproject.toml /pulse-telemetry/
RUN pip3 install .

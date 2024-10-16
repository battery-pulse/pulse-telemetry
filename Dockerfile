FROM docker.stackable.tech/stackable/spark-k8s:3.5.1-stackable24.7.0

# Installs the local project with root user permissions
USER root
WORKDIR /pulse-telemetry
COPY ./src /pulse-telemetry/src
COPY pyproject.toml /pulse-telemetry/
RUN pip install . --no-deps

# changes user back to stackable
USER stackable

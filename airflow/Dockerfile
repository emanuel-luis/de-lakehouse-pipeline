# Build from the repo root: docker build -f airflow/Dockerfile .
ARG AIRFLOW_IMAGE=apache/airflow:slim-3.3.2-python3.12@sha256:a54e7bd6c4d7a17e55cd14b0ead257877b8f8111680d2692cbdba2ff32bd3ff3

FROM ghcr.io/astral-sh/uv:0.13.5 AS uv

# Builder: the pipeline and dbt live in their own venv, isolated from Airflow's dependencies.
FROM ${AIRFLOW_IMAGE} AS builder
USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy
WORKDIR /build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv export --frozen --no-dev --no-emit-project --output-file requirements.txt \
    && uv venv --python "$(command -v python)" /opt/lakehouse-venv \
    && uv pip install --python /opt/lakehouse-venv/bin/python --require-hashes -r requirements.txt \
    && uv build --wheel --out-dir dist \
    && uv pip install --python /opt/lakehouse-venv/bin/python --no-deps dist/*.whl

# Bake dbt packages and the manifest the Cosmos DAG renders from (no dbt call at DAG parse time).
COPY dbt /opt/airflow/dbt
WORKDIR /opt/airflow/dbt
# The sources render env_var("LAKEHOUSE_DATA_DIR") at parse time: use the runtime value.
ENV LAKEHOUSE_DATA_DIR=/opt/airflow/data LAKEHOUSE_DUCKDB_PATH=/tmp/parse.duckdb DO_NOT_TRACK=1
RUN /opt/lakehouse-venv/bin/dbt deps --project-dir . --profiles-dir . \
    && /opt/lakehouse-venv/bin/dbt parse --project-dir . --profiles-dir . \
    && rm -f /tmp/parse.duckdb*

# Final image: Airflow plus Cosmos in Airflow's own environment, under the release constraints.
# No HEALTHCHECK here: one image runs four roles, so the compose healthchecks are the real ones.
FROM ${AIRFLOW_IMAGE}
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
RUN pip install --no-cache-dir --constraint ${AIRFLOW_CONSTRAINTS} \
    "apache-airflow==3.3.2" apache-airflow-providers-standard apache-airflow-providers-postgres \
    "astronomer-cosmos==1.15.1"
USER root
RUN mkdir -p /opt/airflow/data && chown airflow:0 /opt/airflow/data
USER airflow
COPY --from=builder --chown=airflow:0 /opt/lakehouse-venv /opt/lakehouse-venv
COPY --from=builder --chown=airflow:0 /opt/airflow/dbt /opt/airflow/dbt
COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
ENV LAKEHOUSE_DATA_DIR=/opt/airflow/data \
    LAKEHOUSE_DUCKDB_PATH=/opt/airflow/data/warehouse/lakehouse.duckdb \
    DO_NOT_TRACK=1

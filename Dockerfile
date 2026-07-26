ARG AIRFLOW_IMAGE=apache/airflow:3.0.6-python3.12

FROM ${AIRFLOW_IMAGE}

ARG AIRFLOW_VERSION=3.0.6
ARG INSTALL_DEV_DEPENDENCIES=true

COPY --chown=airflow:root \
    requirements.txt \
    requirements-dev.txt \
    /opt/airflow/

RUN if [ "${INSTALL_DEV_DEPENDENCIES}" = "true" ]; then \
        REQUIREMENTS_FILE="/opt/airflow/requirements-dev.txt"; \
    else \
        REQUIREMENTS_FILE="/opt/airflow/requirements.txt"; \
    fi \
    && pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r "${REQUIREMENTS_FILE}" \
    && rm -f \
        /opt/airflow/requirements.txt \
        /opt/airflow/requirements-dev.txt

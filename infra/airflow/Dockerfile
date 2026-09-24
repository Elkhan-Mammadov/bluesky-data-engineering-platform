# Shared Airflow image for airflow-init, airflow-webserver, airflow-scheduler
# and dbt-docs (the only place dbt is installed, per CLAUDE.md: "Airflow +
# dbt in a separate virtualenv").
FROM apache/airflow:2.9.2

# Stay as the image's built-in "airflow" (non-root) user. /opt/airflow
# ($AIRFLOW_HOME) and the airflow user's home (/home/airflow) are the only
# writable locations for that user, so everything below is installed there.
USER airflow

COPY requirements/airflow.txt ${AIRFLOW_HOME}/requirements-airflow.txt
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements-airflow.txt

# dbt lives in its own virtualenv so its dependency set can never collide
# with Airflow's own pinned dependencies. Called as dbt_venv/bin/dbt
# (appended to PATH, not prepended, so Airflow's own "python"/"airflow"
# executables are never shadowed).
RUN python3 -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip
COPY requirements/dbt.txt ${AIRFLOW_HOME}/requirements-dbt.txt
RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements-dbt.txt
ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"

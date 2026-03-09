FROM apache/spark:3.5.0

USER root

COPY pyproject.toml .

WORKDIR /opt/spark/work-dir

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir .

COPY ./src/shared ./src/shared
COPY ./src/jobs/bronze ./src/jobs/bronze
COPY ./src/jobs/silver ./src/jobs/silver

RUN pip install -e .

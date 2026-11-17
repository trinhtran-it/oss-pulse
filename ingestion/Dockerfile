# Ingestion job: GH Archive -> GCS raw.
# Build from the repo root:  docker build -f ingestion/Dockerfile -t oss-pulse-ingestion .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --uid 10001 app
WORKDIR /app

# Dependencies first so code changes don't invalidate this layer.
COPY ingestion/requirements.txt ingestion/requirements.txt
RUN pip install -r ingestion/requirements.txt

COPY ingestion/ ingestion/

USER app
ENTRYPOINT ["python", "-m", "ingestion.gharchive"]
CMD ["--help"]

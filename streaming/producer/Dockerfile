# Producer image: Python + librdkafka (bundled in the confluent-kafka wheel). Runs as non-root.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY streaming/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 app \
    && mkdir /state && chown app /state
COPY streaming/producer/wiki_producer.py .

USER app
VOLUME ["/state"]
ENTRYPOINT ["python", "wiki_producer.py"]

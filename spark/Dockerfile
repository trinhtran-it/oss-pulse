FROM bitnami/spark:3.5

USER root
# GCS connector so Spark can read/write gs:// paths
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/
RUN chmod 644 /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar
USER 1001

COPY flatten_events.py /app/flatten_events.py
WORKDIR /app
ENTRYPOINT ["spark-submit", "/app/flatten_events.py"]

# Flink 2.2 + the connectors the SQL job needs. Each jar is checked against the .sha1 that Maven
# Central publishes next to it, so a corrupted or swapped download fails the build.
FROM flink:2.2.1-java17

ARG KAFKA_CONNECTOR=5.1.0-2.2
ARG JDBC_CONNECTOR=4.1.0-2.2
ARG POSTGRES_DRIVER=42.7.15
# The JDBC connector reports lineage through OpenLineage but doesn't bundle it (its POM: 1.32.0).
ARG OPENLINEAGE=1.32.0
# The Kafka connector doesn't bundle compression codecs. The producer uses zstd, so add the
# zstd-jni version kafka-clients 4.2.0 expects. Without it every fetch fails (NoClassDefFoundError).
ARG ZSTD_JNI=1.5.6-10
ARG MAVEN=https://repo1.maven.org/maven2

RUN set -eux; cd /opt/flink/lib; \
    for path in \
        "org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar" \
        "org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR}/flink-connector-jdbc-core-${JDBC_CONNECTOR}.jar" \
        "org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR}.jar" \
        "org/postgresql/postgresql/${POSTGRES_DRIVER}/postgresql-${POSTGRES_DRIVER}.jar" \
        "io/openlineage/openlineage-java/${OPENLINEAGE}/openlineage-java-${OPENLINEAGE}.jar" \
        "io/openlineage/openlineage-sql-java/${OPENLINEAGE}/openlineage-sql-java-${OPENLINEAGE}.jar" \
        "com/github/luben/zstd-jni/${ZSTD_JNI}/zstd-jni-${ZSTD_JNI}.jar"; \
    do \
        jar="$(basename "$path")"; \
        curl -fsSL -o "$jar" "${MAVEN}/${path}"; \
        echo "$(curl -fsSL "${MAVEN}/${path}.sha1" | cut -c1-40)  ${jar}" | sha1sum -c -; \
    done; \
    chown flink:flink ./*.jar

COPY --chown=flink:flink streaming/flink/sql /opt/sql
COPY --chown=flink:flink --chmod=755 streaming/flink/submit-job.sh /opt/submit-job.sh

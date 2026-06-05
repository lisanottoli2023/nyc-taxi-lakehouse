FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    pandas==2.0.3

# PostgreSQL JDBC driver so Spark can write to Postgres
RUN curl -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY jobs/ /opt/spark/jobs/

WORKDIR /opt/spark/jobs
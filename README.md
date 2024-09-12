# PySpark ETL pipeline

A PySpark ETL to transform a CSV stored on Amazon S3 and orchestrated using Apache Airflow.
## Dockerfile

The `docker-compose.yaml` file has been tested with version 27.2.0 of Docker. The Dockerfile extends the [Apache Airflow image](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) to install additional [dependencies](#Dependencies) for PySpark to run.
## Dependencies

- Java JDK 15.0.2
- Apache Hadoop 3.3.6
- Spark 3.5.2
- Airflow 2.10.1

pip installs can be found [here](https://github.com/swandrn/pyspark-pipeline/blob/master/requirements.txt).
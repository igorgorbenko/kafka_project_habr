# kafka_project_habr

The example of usage Spark Streaming with Apache Kafka. 

#### Execution:

```console
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,\
org.postgresql:postgresql:9.4.1207 \
spark_job.py localhost:9092 transaction
```

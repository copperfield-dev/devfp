./spark-3.1.2/bin/spark-submit \
   --class com.nd.data.ingestion.KafkaImporter \
   --master yarn   \
   --deploy-mode client \
   --conf spark.yarn.maxAppAttempts=4  \
   --conf spark.yarn.am.attemptFailuresValidityInterval=1h  \
   --conf spark.yarn.max.executor.failures=8 \
   --conf spark.yarn.executor.failuresValidityInterval=1h  \
   --conf spark.task.maxFailures=8 \
   --conf spark.speculation=true \
   --principal wuxin@LZ.DSCC.99.COM \
   --keytab /home/wuxin/wuxin.keytab \
   --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
   /home/wuxin/data-ingestion-0.0.3-jar-with-dependencies.jar \
   devfp_kafka_test \
   earliest >> /home/wuxin/devfp_ingestion.log 2>&1

   --deploy-mode clusterl l
   --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
   --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties  \
   --files /path/to/log4j.properties \
   --conf spark.metrics.conf=/home/wuxin/spark-3.1.2/conf/metrics.properties \


./spark-3.1.2/bin/spark-submit \
   --class com.nd.data.ingestion.DayDeduplication \
   --master yarn --deploy-mode client \
   /home/wuxin/data-ingestion-0.0.3-jar-with-dependencies.jar \
   2022-05-09

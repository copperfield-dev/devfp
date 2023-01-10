package com.nd.data.ingestion

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, StreamingContext}

import java.time.format.DateTimeFormatter
import java.util.Locale
import java.io.IOException
import java.time.{LocalDate, Instant}

/**
 * Created by copperfield @ 2022/5/9 15:49   
 */
object KafkaImporter {

  private def login(): Unit = {
    System.setProperty("java.security.krb5.conf", "/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")
    val configuration = new Configuration()

    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/core-site.xml")
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/hdfs-site.xml")
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/yarn-site.xml")
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/hive-site.xml")

    configuration.set("hadoop.security.authentication", "kerberos")

    UserGroupInformation.setConfiguration(configuration)
    try {
      UserGroupInformation.loginUserFromKeytab("hive/longzhou-hdpnn.lz.dscc.99.com@LZ.DSCC.99.COM", "/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/hive.keytab")
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: group.id & auto.offset.reset is needed!")
      System.exit(0)
    }

    val group_id = args(0)
    val auto_offset_reset = args(1)

    //    KafkaImporter.login()

    val conf = new SparkConf()
      //      .setMaster("local[4]")
      .setAppName("sdk_devices_ingestion")
      .set("spark.dynamicAllocation.minExecutors", "16")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "8400")
      .set("spark.sql.orc.impl", value = "hive")
      .set("spark.sql.caseSensitive", value = "false")
      .set("spark.metrics.namespace", "${spark.app.name}")

    val ssc = new StreamingContext(conf, Minutes(20))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.24.140.162:9195,172.24.140.163:9195,172.24.140.165:9195",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> auto_offset_reset,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.mechanism" -> "SCRAM-SHA-256",
      "sasl.jaas.config" -> "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sdkdevices-read\" password=\"bc023bad9cbf7da4a840110cbe3cfd04bc238886bba0830a771db0c71e9ddf6c\";"
    )

    val topics = Array("sdk_devices")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val hdfsPath = "hdfs://bigdata-test/user/wuxin/devfp_data/"

    //    stream.map(record => (record.key, record.value))
    //      .saveAsNewAPIHadoopFiles(hdfsPath + "/" + strDate + "/data", "json", classOf[Text], classOf[Text], classOf[TextOutputFormat[_, _]])


    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        /* 写一份原始数据到HDFS */
        val now = LocalDate.now
        val strDate = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.CHINESE))
        val timestamp = Instant.now.toEpochMilli
        rdd.map(record => (record.key, record.value))
          .saveAsTextFile(s"${hdfsPath}/${strDate}/data-${timestamp}.json")

        /* 手动提交offset */
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start
    ssc.awaitTermination
  }

}

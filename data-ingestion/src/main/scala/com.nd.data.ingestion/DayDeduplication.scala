package com.nd.data.ingestion

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array, col, concat_ws, current_date, current_timestamp, date_format, date_sub, expr, md5}

import java.io.IOException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.elasticsearch.spark.sql._

/**
 * Created by copperfield @ 2022/5/11 17:15   
 */
object DayDeduplication {
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
        ex.printStackTrace();
    }
  }

  def main(args: Array[String]): Unit = {
    //    DayDeduplication.login()

    val yesterday = LocalDate.now.minusDays(1)
    var strDate = yesterday.format(DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.CHINESE))

    if (args.length == 1) {
      strDate = args(0)
    }

    val spark = SparkSession
      .builder()
      //      .master("local[4]")
      .appName("day-deduplication")
      .config("spark.sql.caseSensitive", value = false)
      .enableHiveSupport()
      .getOrCreate()



    /* 用HDFS上的新数据和Hive上的历史数据进行比对 */

    val path = s"hdfs://bigdata-test/user/wuxin/devfp_data/${strDate}/*.json"
    //    val path = "file:///Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/test/resources/data.json"
    val new_raw_data = spark.read.json(path)
    var new_df = new_raw_data.select(col("ndDevice.*"), col("ydDevice.*"))

    /* 新数据预处理: 类型转换, 空值填充 */
    val new_devfp_columns = new_df.columns.map(c => c.toLowerCase())
    val columns = new_devfp_columns.map(c => col(c).cast("string"))

    new_df = new_df.toDF(new_devfp_columns: _*).select(columns: _*)
      .na.replace("*", Map("" -> "None"))
      .na.fill("None")
      .withColumn("md5_id", md5(concat_ws("|", columns: _*)))

    import spark.sql
    /* 从Hive表获取旧数据 */
    val old_df = sql(s"SELECT * FROM devfp WHERE process_date < \'$strDate\'")
      .withColumn("md5_id", md5(concat_ws("|", columns: _*)))

    val valid_md5_df = new_df.select("md5_id").except(old_df.select("md5_id"))
    val insert_df = new_df.join(valid_md5_df, Seq("md5_id"), "left_semi")
      .drop("md5_id")

    /* 将无重复的新增数据写入Hive新的日期分区 */
    insert_df.withColumn("process_date", date_sub(current_date(), 1))
      .write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("tmp_devfp")
    val sqlStr = s"INSERT INTO TABLE devfp PARTITION(process_date=\'${strDate}\') " + s"SELECT ${columns.mkString(", ")} FROM tmp_devfp"
    spark.sql(sqlStr)
    spark.sql("DROP TABLE tmp_devfp")

    /* 将无重复的新增数据写入ElasticSearch新的日期索引 */
    val index_prefix = "log-devfp-details"
    val es_options = Map(
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "inview-ls-lz.99.com",
      "es.port" -> "80",
      "es.net.http.auth.user" -> "log-writer",
      "es.net.http.auth.pass" -> "Oi9A27HoMqdVO2uu"
    )

    val insert_es_df = new_df.withColumn("process_time", date_format(current_timestamp() - expr("INTERVAL 24 HOURS"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    //    insert_es_df.show(false)

    insert_es_df.saveToEs(s"log-devfp-details-${strDate}/_doc", es_options)

    spark.stop()
  }
}

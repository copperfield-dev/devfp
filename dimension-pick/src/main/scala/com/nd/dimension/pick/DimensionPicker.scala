package com.nd.dimension.pick

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.io.IOException
import scala.collection.mutable.ListBuffer

/**
 * Created by copperfield @ 2022/4/27 15:47   
 */
object DimensionPicker {
  
  private def login(): Unit = {
    System.setProperty("java.security.krb5.conf", "/Users/copperfield/Documents/IdeaProjects/devfp/data-ingestion/src/main/resources/krb5.conf");
    System.setProperty("sun.security.krb5.debug", "true");
    val configuration = new Configuration()
    
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/dimension-pick/src/main/resources/core-site.xml");
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/dimension-pick/src/main/resources/hdfs-site.xml");
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/dimension-pick/src/main/resources/yarn-site.xml");
    configuration.addResource("/Users/copperfield/Documents/IdeaProjects/devfp/dimension-pick/src/main/resources/hive-site.xml");
    
    configuration.set("hadoop.security.authentication", "kerberos");
    
    UserGroupInformation.setConfiguration(configuration);
    try {
      UserGroupInformation.loginUserFromKeytab("hive/longzhou-hdpnn.lz.dscc.99.com@LZ.DSCC.99.COM", "/Users/copperfield/Documents/IdeaProjects/devfp/dimension-pick/src/main/resources/hive.keytab")
    } catch {
      case ex: IOException =>
        ex.printStackTrace();
    }
  }
  
  def main(args: Array[String]): Unit = {
    
//    DimensionPicker.login()
    
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("sdk_devices_pick_dimension")
      .enableHiveSupport()
      .getOrCreate()
  
    /* 读取样本数据 */
    var df = spark.sql("SELECT yddeviceid, " +
      "androidid, " +
      "applist, " +
      "carrier, " +
      "clientip, " +
      "country, " +
      "cpu, " +
      "cputype, " +
      "deviceboard, " +
      "devicebrand, " +
      "devicemanufacturer, " +
      "devicemodel, " +
      "devicename, " +
      "devicetype, " +
      "firmwareversion, " +
      "idfa, " +
      "imei, " +
      "imsi, " +
      "language, " +
      "mac, " +
      "networkmode, " +
      "oaid, " +
      "resolution, " +
      "systemname, " +
      "systemversion, " +
      "timezone " +
      "from devfp")
    
    
    
    val encodingColumns = Array(
      "androidid", "carrier", "clientip", "country",
      "cpu", "cputype", "deviceboard", "devicebrand", "devicemanufacturer",
      "devicemodel", "devicename", "firmwareversion", "idfa",
      "imei", "imsi", "language", "mac", "networkmode", "oaid",
      "resolution", "systemname", "systemversion", "timezone")
  
    df = df.na.replace(encodingColumns, Map("" -> "None"))
    df.show(1)
    
    val stages = new ListBuffer[PipelineStage]()
    
    /* 标签编码 */
    val labelIndexer = new StringIndexer().setInputCol("yddeviceid").setOutputCol("yddeviceid_index")
    stages.append(labelIndexer)
    
    /* 特征值编码 */
    
    for (column <- encodingColumns) {
      val indexer = new StringIndexer().setInputCol(column).setOutputCol(s"${column}_index")
      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${column}_vec")
      stages.append(indexer, encoder)
    }
    
//    val numericCols = Array()
    val assemblerInputs = encodingColumns.map(_ + "_vec")
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stages.append(assembler)
    
    val pipeline = new Pipeline()
    pipeline.setStages(stages.toArray)
    
    val pipelineModel = pipeline.fit(df)
    var dataset = pipelineModel.transform(df)
    dataset.show(false)
    dataset = dataset.select(col("yddeviceid").alias("label"), col("features"))
  
    val hdfsPath = "hdfs://bigdata-test/user/wuxin/devfp_svm_data/"
    dataset.write.save(hdfsPath)
    
    
    /* 载入样本数据 */
//    val datapath =
//    val data = MLUtils.loadLibSVMFile(spark, datapath)
    
    /* 划分训练集&测试集 */
//    val splits = data.randomSplit(Array(0.6,0.4),seed=1L)
//    val training = splits(0)
//    val testing = splits(1)
  
    /* 新建决策树, 并设置训练参数 */
    // val model = DecisionTree.trainClassifier(training, 2, Map[Int,Int](), "gini", 5, 32)
    
  }
  
}

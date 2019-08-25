package org.ermilov

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream


object SparkWriteApplication extends App {
  val config = new SparkConf().setMaster("spark://spark-master:7077").setAppName("SparkWriteApplication")
    .set("spark.driver.memory", "2g")
    .set("spark.executor.memory", "2g")
    .set("spark.executor.memoryOverhead", "2g")
    .set("spark.executor.cores", "7")
    .set("spark.cores.max", "8")
    .set("spark.executor.instances", "1")

  val spark = SparkSession
      .builder()
//      .master("local")
      .config(config)
//      .appName("App")
      .getOrCreate()

  val sc = new SparkContext(config)

//  val stream: DStream[String] = KafkaConsumerFactory.createKafkaMessageStream(Array("bitcoin"), ssc).map(record => record.value())
//  stream.saveAsTextFiles("hdfs://namenode:8020/bitcoin/topic")

    val loadRdds: DataFrame = spark.read.json("hdfs://namenode:8020/user/db/test/seyed_test")
    loadRdds.show()


}


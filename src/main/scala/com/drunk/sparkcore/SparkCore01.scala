package com.drunk.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark在IDE的运行模式
 * local[*]使用本地模式：是windows本地的spark
 *
 * 打jar包模式，到集群提交。使用的是liunx的分布式集群。
 */

object SparkCore01 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("drunk")
    var sc = new SparkContext(conf)
    sc.textFile("hdfs://192.168.110.148:8020/hello.log")
      .flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2)
      .saveAsTextFile("hdfs://192.168.110.148:8020//sparkout1")
    sc.stop()
  }
}

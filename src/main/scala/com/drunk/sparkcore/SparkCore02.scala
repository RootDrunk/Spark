package com.drunk.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore02 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("drunk")
    var sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("hdfs://192.168.110.148:8020/hello.log")
    val rdd1: RDD[String] = rdd.flatMap(_.split(","))
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val rdd4: RDD[(String, Int)] = rdd3.sortBy(_._2)
    rdd4.saveAsTextFile("hdfs://192.168.110.148:8020/sparkoutput2")

    sc.stop()
  }
}

package com.drunk.sull

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Nulls {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("hdfs://192.168.110.148:8020/nullid")
    value.map(lines=> {
      val str: Array[String] = lines.split("\t")
      (str(0),1)
    })
      .reduceByKey(_+_)

      .collect
      .foreach(println)

  }
}

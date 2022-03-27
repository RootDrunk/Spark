package com.drunk.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object one1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("one1")
    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("hdfs://192.168.110.148:8020/ip.txt")
   value.map(lines => {
      val data: Array[String] = lines.split('|')
      ((data(6), data(7)), 1)
    })
      .reduceByKey(_ + _)
      .map {
        case ((x, y), z) => (x, (y, z))
      }
      .groupByKey()
      .map{
        case (x,y)=>
          (x, y.toList.sortBy(_._2)(Ordering.Int.reverse).take(1))
      }
      .collect
      .foreach(println)
  }
}

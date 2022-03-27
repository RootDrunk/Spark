package com.drunk.sparkcore


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd 中的 transformation 转换算子
 * map 对rdd中的每个元素进行映射
 */
object SparkRDD2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("drunk1")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd1: RDD[Int] = rdd.map(x => {
      x * 10
    })
    rdd1.foreach(x=>print(x+"\t"))

    println("\n=======================================")

    val value: RDD[Int] = rdd.map(_ * 10)

    var rdd2 = rdd.map{
      (x)=>{
       x*10
      }
    }
    rdd2.foreach(x=>print(x+"\t"))
  }
}

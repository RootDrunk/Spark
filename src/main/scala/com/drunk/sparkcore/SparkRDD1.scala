package com.drunk.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * rdd 弹性分布式数据集
 * rdd 的创建方式：1：通过函数创建
 * parallelize  makeRDD
 *  rdd 的创建方式：2：通过读取数据，获取RDD
 *  sc.textFile();
 *
 *  rdd 的创建方式：3：通过转化获取到RDD
 */
object SparkRDD1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("drunk1")
    val sc = new SparkContext(conf)
    // 通过集合创建RDD  parallelize
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    // 使用foreach行动算子，打印rdd中的元素
    rdd1.foreach(x=>print(x+"\t"))
    println("\n===============================")

    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 43, 5))
    rdd2.foreach(x=>print(x+"\t"))

    println("\n===============================")
    // 通过读取文件，获取RDD
    val rdd3: RDD[String] = sc.textFile("")

    println("\n===============================")
    // 通过转化，获取RDD
    val rdd5: RDD[Int] = rdd2.map(x => x * 10)
    rdd5.foreach(println(_))
  }
}

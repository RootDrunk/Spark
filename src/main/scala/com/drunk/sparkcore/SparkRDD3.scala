package com.drunk.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("drunk1")
    val sc = new SparkContext(conf)

    def m1(iter:Iterator[Int]):Iterator[Int]={
       // 通过函数，让集合中的每个元素：乘以10
      var res = List[Int]()
      while(iter.hasNext){
        val cur = iter.next()
        res.::=((cur*10))
      }
       res.iterator
    }

    val rdd: RDD[Int] = sc.makeRDD(List(4, 5, 6, 7, 8, 9), 3)
    val rdd1: RDD[Int] = rdd.mapPartitions(m1)
    rdd1.foreach(println(_))
  }
}

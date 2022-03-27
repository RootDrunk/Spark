package com.drunk.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object index1 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("index1")
    var sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://192.168.110.148:8020/teacher.log")
    //        val value: RDD[(String, Int)] = lines.map(line => {
    //          val str: String = line.split('/')(3)
    //          (str, 1)
    //        })
    //        val result: RDD[(String, Int)] = value.reduceByKey(_+_).sortBy(_._2, false)
    //        println("\n"+result.collect().toBuffer)
    //
    lines.map(line => {
      val str1: String = line.split('/')(2)
      val str2: String = str1.split('.')(0)
      val str3: String = line.split('/')(3)
      ((str2, str3), 1)
    })
      .reduceByKey(_ + _)
      .map {
        case ((x, y), z) => {
          (x, (y, z))
        }
      }
      .groupByKey()
      .map {
        case (x, y) => {
          (x, y.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
        }
      }
      .collect
      .foreach(println)


    //    lines.map(
    //      line => {
    //        val data: Array[String] = line.split("\\.")
    //        ((data(0), data(2)), 1)
    //      }
    //    )
    //      .reduceByKey(_+_)
    //      .map{
    //        case ((x, y), z) => {
    //          (x, (y, z))
    //        }
    //      }
    //      .groupByKey()
    //      .map {
    //        case (x, y) => {
    //          (x, y.toList.sortBy(_._2)(Ordering.Int.reverse).take(1))
    //        }
    //      }
    //      .collect
    //      .foreach(println)


    //    lines.map(x => {
    //    val data: Array[String] = x.split("\\.")
    //    ((data(0), data(2)), 1)
    //   })
    //    .reduceByKey(_ + _)
    //    .map {
    //     case ((subject, teacher), sum) => {
    //      (subject, (teacher, sum))
    //     }
    //    }
    //    .groupByKey()
    //    .map(x => {
    //     (x._1, x._2.toList.sortBy(_._2)(Ordering.Int.reverse).head)
    //    })
    //    .collect
    //    .foreach(println)

  }
}

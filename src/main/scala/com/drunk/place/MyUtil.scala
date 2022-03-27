package com.drunk.place

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Flow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.io.{BufferedSource, Source}

object MyUtil {

  // 将IP转换成10进制表示形式。
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }



  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def main(args: Array[String]): Unit = {
    //数据是在内存中
//    val rules: Array[(Long, Long, String)] = readRules("/home/devli/vm共享/04sparkIP归属地/ip.txt")
//
//    var conf = new SparkConf().setMaster("local[*]").setAppName("index1")
//    var sc = new SparkContext(conf)
//
//    val value: RDD[String] = sc.textFile("/home/devli/vm共享/04sparkIP归属地/access.log")
//    value.map(line => {
//      val str: Array[String] = line.split('|')
//      val vas: String = rules(binarySearch(rules, ip2Long(str(1))))._3
//      (vas, 1)
//    })
//      .reduceByKey(_ + _)
//      .groupByKey()
//      .collect
//      .foreach(println)

       val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparksql")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val rules: Array[(Long, Long, String)] = readRules("/home/devli/vm共享/04sparkIP归属地/ip.txt")
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile("/home/devli/vm共享/04sparkIP归属地/access.log")
//    val value: RDD[Flow] = rdd.map(line => {
//      val str: Array[String] = line.split('|')
//      val vas: Long = rules(binarySearch(rules, ip2Long(str(1))))._3.toLong
//      new Flow(vas, 1)
//    })
//        val flowDS: Dataset[Flow] = rdd.map(x => {
//         val str: Array[String] = x.split('|')
//          val vas: Long = rules(binarySearch(rules, ip2Long(str(1))))._3.toLong
//            new Flow(vas,1)
//    })


    //    //将ip地址转换成十进制
    //    val ipNum = ip2Long("125.213.100.123")
    //    println(ipNum)
    //    //查找
    //    val index = binarySearch(rules, ipNum)
    //    //根据脚本到rules中查找对应的数据
    //    val tp = rules(index)
    //    val province = tp._3
    //    println(province)

  }
}

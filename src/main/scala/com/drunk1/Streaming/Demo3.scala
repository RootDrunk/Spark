package com.drunk1.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("ssm")

    var ssc = new StreamingContext(conf,Seconds(5))

    val ds: DStream[String] = ssc.textFileStream("hdfs://192.168.110.148:8020/sscdata")
    val ds1: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    ds1.print

    // 启动流式程序
    ssc.start()
    ssc.awaitTermination()


  }
}

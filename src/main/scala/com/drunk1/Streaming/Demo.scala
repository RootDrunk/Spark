package com.drunk1.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo {
  def main(args: Array[String]): Unit = {
    //定义更新状态方法，参数value为当批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val conf = new SparkConf().setMaster("local[*]").setAppName("drunk").set("spark.streaming.stopGracefullyOnShutdown","true")
    val sc = new StreamingContext(conf,Seconds(3))
    sc.checkpoint(".")
    val linux: ReceiverInputDStream[String] = sc.socketTextStream("192.168.110.148", 9999)
    val value: DStream[String] = linux.flatMap(x => {
      x.split(" ")
    })
    value.map((_,1)).reduceByKey(_+_).updateStateByKey[Int](updateFunc).print()
    sc.start()
    sc.awaitTermination()
  }
}

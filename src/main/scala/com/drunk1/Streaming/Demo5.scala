package com.drunk1.Streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import java.net.InetSocketAddress
//
//object Demo5 {
//  def main(args: Array[String]): Unit = {
//    var conf = new SparkConf().setMaster("local[*]").setAppName("drunk")
//    var ssc = new StreamingContext(conf, Seconds(5))
//    val address = Seq(new InetSocketAddress("192.168.110.148", 55555))
//    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK_SER_2)
//    val lineDstream: DStream[String] = stream.map(x => new String(x.event.getBody.array()))
//    val wordAndOne: DStream[(String, Int)] = lineDstream.flatMap(_.split(" ")).map((_, 1))
//    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
//    result.print()
//
//    // 启动流式程序
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//  }
//}






//package com.dahua.sparkstreaming01

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.InetSocketAddress

object Demo5 {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("sqq")
    val conf1 = new SparkConf()
    var ssc = new StreamingContext(conf, Seconds(5))
    val address = Seq(new InetSocketAddress("192.168.110.148", 55555))
    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK_SER_2)
    val lineDstream: DStream[String] = stream.map(x => new String(x.event.getBody.array()))
    val wordAndOne: DStream[(String, Int)] = lineDstream.flatMap(_.split(" ")).map((_, 1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    result.print()

    // 启动流式程序
    ssc.start()
    ssc.awaitTermination()


  }

}

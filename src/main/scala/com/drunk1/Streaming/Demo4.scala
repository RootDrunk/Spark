package com.drunk1.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo4 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("ssm")

    var ssc = new StreamingContext(conf,Seconds(5))

    // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
    // 4.1.配置kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.110.148:9092,192.168.137.67:9092,192.168.137.68:9092",
      "group.id" -> "kafka_Direct")
    // 4.2.定义topic
    val topics = Set("first")

    // 直连方式
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    // 5.获取topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)

    // 6.切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
    // 7.相同单词出现的次数累加
    val resultDS: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    // 8.通过Output Operations操作打印数据
    resultDS.print()
    // 9.开启流式计算
    ssc.start()
    // 阻塞一直运行
    ssc.awaitTermination()



    // 启动流式程序
    ssc.start()
    ssc.awaitTermination()


  }
}

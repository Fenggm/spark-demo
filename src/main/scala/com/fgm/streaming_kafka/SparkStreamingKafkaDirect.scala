package com.fgm.streaming_kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *利用sparkStreaming整合kafka，不再有zk去维护消息的偏移量，由客户端程序自己去维护
  * @Auther: fgm
  */
object SparkStreamingKafkaDirect {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("kafkaDirect").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //创建streamingContext
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("/sparkDirect")
    val kafkaParams = Map("bootstrap.servers"->"node01:9092,node02:9092,node03:9092","group.id"->"spark-direct")
    val topics=Set("kafkaSpark")
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    //获取topic的内容
    val data: DStream[String] = kafkaDstream.map(_._2)
    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()


  }

}

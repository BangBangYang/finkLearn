package com.bupt.sinkTest



import java.util.Properties

import com.bupt.apiTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}



/**
 * @author yangkun
 * @date 2020/11/30 21:25
 * @version 1.0
 */
object KafkaTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    val inputStream: DataStream[String] = env.readTextFile("input/sensor.txt")
val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp4.buptnsrc.com:6667")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("a",new SimpleStringSchema(),properties))
//    inputStream.print()
//    //1. 封装成SensorReading
    val mapStream: DataStream[String] = inputStream.map {
      data => {
        val arrays = data.split(",")
        SensorReading(arrays(0), arrays(1).trim().toLong, arrays(2).trim().toDouble).toString
      }
    }
    mapStream.print()
    mapStream.addSink(new FlinkKafkaProducer011[String]("hdp4.buptnsrc.com:6667","ads-yk",new SimpleStringSchema()))
   env.execute()
  }

}

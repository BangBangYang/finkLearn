package com.bupt.apiTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * @author yangkun
 * @date 2020/11/27 20:46
 * @version 1.0
 */
object SourceTeset {
  def main(args: Array[String]): Unit = {
    val dataList = List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
      )
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1. 集合读取
    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
//    stream1.print()
    //2. 文件读取
    val stream2: DataStream[String] = env.readTextFile("input/sensor.txt")
    stream2.print()
    //3.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdp4.buptnsrc.com:6667")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("a",new SimpleStringSchema(),properties))
    stream3.print()
    //4. 自定义源
    //除了以上的source数据来源，我们还可以自定义source。需要做的，只是传入一个SourceFunction就可以。具体调用如下：

//    val steam4: DataStream[SensorReading] = env.addSource(new mySensorSource)
//    steam4.print()
    //执行
    env.execute("source test")

  }
}
// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

class mySensorSource extends SourceFunction[SensorReading]{
  // flag: 表示数据源是否还在正常运行
  var running = true
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()
    //随机生成一组（10）个传感器温度:（id, temp）
    var curTemp = 1.to(10).map( i =>("sensor_"+i,rand.nextDouble()*100))
    //定义无限循环,不停产生数据，除非被cancel
    while(running){
        //在上次数据基础上微调更新温度值
      curTemp = curTemp.map(
        data => (data._1,data._2 + rand.nextGaussian())
      )
      //获取当前时间戳,加入到数据中,调用ctx.collect发出数据
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1,curTime,data._2))
      )

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}

package com.bupt.apiTest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author yangkun
 * @date 2020/12/8 20:23
 * @version 1.0
 */
object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.socketTextStream("hdp4.buptnsrc.com",9999)

    val stream = inputStream.map(
      line =>{
        val fields: Array[String] = line.split(",")
        SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
      }
    )
        //必须使用毫秒数  乱序数据
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)){
          override def extractTimestamp(element: SensorReading) = element.timestamp* 1000L
        })
    val resStream = stream.keyBy(_.id)
        .timeWindow(Time.seconds(10))
         //允许延迟1分钟收到数据
        .allowedLateness(Time.minutes(1))
        //延迟超过一分钟的进入侧流
        .sideOutputLateData(new OutputTag[SensorReading]("late-data"))
        .reduce(new MyMinTemp())
    resStream.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")
    resStream.print("res")
    env.execute("waterMark")
  }

}
// 自定义取窗口最小温度值的聚合函数
class MyMinTemp extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = SensorReading(value1.id,value2.timestamp,value2.temperature.min(value1.temperature))
}

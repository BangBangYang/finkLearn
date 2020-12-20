package com.bupt.apiTest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author yangkun
 * @date 2020/12/7 16:01
 * @version 1.0
 */
object WindowsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

   val inputStream = env.socketTextStream("hdp4.buptnsrc.com",9999)
   val stream = inputStream.map(
     line =>{
       val fields: Array[String] = line.split(",")
       SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
     }
   )
//    stream.print()
    stream
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(10)) //简化版开窗函数  10秒大小的滚动窗口
      // .window( EventTimeSessionWindows.withGap(Time.seconds(1)) )    // 会话窗口
      // .window( TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 带10分钟偏移量的1小时滚动窗口
      // .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 1小时窗口，10分钟滑动一次
      // .countWindow( 10, 2 )    // 滑动计数窗口
      .minBy(2)
//      .reduce((state, newData) => SensorReading(state.id,newData.timestamp+1,newData.temperature.min(state.temperature)))
//      .reduce(new MyMaxTemp)
      .print()

    env.execute()
  }

}
// 自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}
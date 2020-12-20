package com.bupt.apiTest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2020/12/16 19:24
 * @version 1.0
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.socketTextStream("hdp4.buptnsrc.com",9999)

//    val stream = inputStream.map(
//      line =>{
//        val fields: Array[String] = line.split(",")
//        SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
//      }
//    ).keyBy(_.id)
//      .process(new MyKeyedProcess)

    val stream = inputStream.map(
      line =>{
        val fields: Array[String] = line.split(",")
        SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
      }
    )
    val warningStream = stream.keyBy(_.id).process(new TempIncreWarning(10000L))
    warningStream.print()
    env.execute("ProcessFunction Test...")
  }

}
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String,SensorReading,String]{
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  lazy val currentTimerState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    val lastTemp = lastTempState.value()
    val timerTs = currentTimerState.value()
    lastTempState.update(value.temperature)
//    println(ctx.timerService().currentProcessingTime())
//    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+10000L)
    if(value.temperature > lastTemp && timerTs == 0){

      val ts = ctx.timerService().currentProcessingTime()+interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      currentTimerState.update(ts)
    }
    else if(value.temperature < lastTemp){
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      currentTimerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit ={
    out.collect("传感器"+ctx.getCurrentKey+"的温度连续"+interval/1000+"秒上升了")
    currentTimerState.clear()
  }

}
//自定义Keyed Process Function,实现10秒内温度连续上升警报检测
//class TempIncreWarning(interval:Long) extends  KeyedProcessFunction[String,SensorReading,String]{
//  // 定义状态：保存上一个温度值进行比较，保存注册定时器的时间用于删除
//  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
//  lazy val currTimerState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer-ts",classOf[Long]))
//
//  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
//    //先取状态
//    val lastTemp = lastTempState.value()
//    val timerTs = currTimerState.value()
//    //更新温度值
//    lastTempState.update(value.temperature)
////    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+interval)
//    //当前温度值和上次温度进行比较
//    if(value.temperature > lastTemp && timerTs == 0){
//      // 如果温度上升且没有定时器，那么注册当前数据时间戳10之后的定时器
//      val ts = ctx.timerService().currentProcessingTime()+interval
//      ctx.timerService().registerProcessingTimeTimer(ts)
//      currTimerState.update(ts)
//
//    }else if(value.temperature < lastTemp){
//      //如果温度下降，删除定时器
//      ctx.timerService().deleteProcessingTimeTimer(timerTs)
//      currTimerState.clear()
//
//    }
//
//  }
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
//    out.collect("传感器"+ctx.getCurrentKey+"的温度连续"+interval/1000+"秒上升了")
//    currTimerState.clear()
//  }
//}
class MyKeyedProcess extends KeyedProcessFunction[String,SensorReading,String]{
  var myState:ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("mystate",classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp()
    ctx.timerService().currentWatermark()
    ctx.timerService().registerProcessingTimeTimer(ctx.timestamp()+60000L)
//    ctx.timerService().deleteEventTimeTimer()
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
}

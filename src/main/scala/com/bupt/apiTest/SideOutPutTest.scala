package com.bupt.apiTest

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2020/12/17 16:56
 * @version 1.0
 */
object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend(new FsStateBackend("FsStateBackend"))
    val inputStream: DataStream[String] = env.socketTextStream("hdp4.buptnsrc.com",9999)
    val dataStream = inputStream.map(line =>{
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
    })
    // 用侧输出流实现分流操作，定义主流为高温流
    val highStream = dataStream.process(new SplitStreamOperation(30.0))
    highStream.print("high")
    val lowStream = highStream.getSideOutput(new OutputTag[(String,Double,Long)]("low"))
    lowStream.print("low")
    env.execute("")
  }

}
//自定义ProcessFunction，实现高低温分流操作
class SplitStreamOperation(threshold: Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if(value.temperature > threshold){
      //如果大于阀值，直接输出到主流(高温流)
      out.collect(value)
    }else{
      //如果小于等于，输出到侧输出流
      ctx.output(new OutputTag[(String,Double,Long)]("low"),(value.id,value.temperature,value.timestamp))
    }
  }
}

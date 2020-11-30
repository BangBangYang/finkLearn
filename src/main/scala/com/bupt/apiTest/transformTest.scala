package com.bupt.apiTest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author yangkun
 * @date 2020/11/30 11:08
 * @version 1.0
 1、keyBy算子
 1.1 keyBy将DataStream → KeyedStream
   KeyedStream才有滚动聚合算子
   sum,min,minBy,max,maxBy
   这些聚合滚动算子又将KeyedStream -> DataStream
 1.2 min 和 minBy区别
   min 只更新单个字段，其余字段跟第一次出现的一样
   minBy 更新所有字段
 1.3 reduce
 KeyedStream → DataStream
一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果
reduce 有两种传参方式，一种是通过lambda表达式，一种是通过自定义类，自定义类必须继承ReduceFunction

2、分流
2.1 split
DataStream → SplitStream
根据某些特征把一个DataStream拆分成两个或者多个DataStream
2.2 select
SplitStream→DataStream：
从一个SplitStream中获取一个或者多个DataStream

3、合流
3.1 connect
DataStream,DataStream → ConnectedStreams
连接两个保持他们类型的数据流，两个数据流被Connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立
3.2 CoMap,CoFlatMap
ConnectedStreams → DataStream
作用于ConnectedStreams上，功能与map和flatMap一样，对ConnectedStreams中的每一个Stream分别进行map和flatMap处理
3.3 Connect与 Union 区别：
    1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
    2. Connect只能操作两个流，Union可以操作多个。

 */
object transformTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.readTextFile("input/sensor.txt")
    //1. 封装成SensorReading
    val mapStream: DataStream[SensorReading] = inputStream.map {
      data => {
        val arrays = data.split(",")
        SensorReading(arrays(0), arrays(1).trim().toLong, arrays(2).trim().toDouble)
      }
    }
    //2. 分组聚合，输出每个传感器的当前的最小值
    val aggtream: DataStream[SensorReading] = mapStream
        //根据id分组
        .keyBy("id")
//      .min("temperature")
        .minBy("id")
    //3. 需要输出当前最小的温度值以及最大的时间戳，要用reduce
    val resDstream: DataStream[SensorReading] = mapStream
      .keyBy("id")
      //reduce 自定义类实现方式
      .reduce(new MyReduceFunction)
      //reduce lambda表达式实现方式
//      .reduce(
//        (s1, s2) => SensorReading(s1.id, s2.timestamp, s2.temperature.min(s1.temperature))
//      )


    //4. 多流转化操做
    // 4.1 分流，将传感器温度转化为高温和低温两种流
    val splitStream: SplitStream[SensorReading] = mapStream.split(
      data => {
        if (data.temperature > 30.0) Seq("high") else Seq("low")

      }
    )
    val highTempStream= splitStream.select("high")
    val lowTempStream = splitStream.select("high")
    val allTempStream = splitStream.select("high","low")
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
    //4.2 合流 connect
    val warningStream = highTempStream.map(data => (data.id,data.temperature))
    val connectedStreams = warningStream.connect(lowTempStream)
    val conMapStream: DataStream[Product] = connectedStreams.map(
      warningdata => (warningdata._1, warningdata._2, "warning"),
      lowdata => (lowdata.id, "healthy")
    )
//    conMapStream.print()
   //4.3 union

    val unionStream = highTempStream.union(lowTempStream)
    unionStream.print()


    env.execute()

  }

}
// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

class MyReduceFunction extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t1.timestamp,t.temperature.min(t1.temperature))
  }
}
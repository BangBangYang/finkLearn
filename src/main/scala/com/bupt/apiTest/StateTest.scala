package com.bupt.apiTest

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2020/12/14 21:11
 * @version 1.0
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.socketTextStream("hdp4.buptnsrc.com",9999)

    val stream = inputStream.map(
      line =>{
        val fields: Array[String] = line.split(",")
        SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
      }
    )
    //需求:对于温度传感器温度值跳变，超过10度，报警
    val alerStream = stream.keyBy(_.id)
//        .flatMap(new TempChageAlert(10.0))
        .flatMapWithState[(String,Double,Double),Double]{
          case (data:SensorReading,None) => (List.empty,Some(data.temperature))
          case (data:SensorReading,lastTemp:Some[Double]) => {

            //跟最新的温度求差值作比较
            val diff = (data.temperature - lastTemp.get).abs
            if(diff > 10.0)
              (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
            else{
              (List.empty,Some(data.temperature))
            }
          }
        }
    alerStream.print()

    env.execute("state test")
  }

}
//实现自定义RichFlatMapFuncion
class TempChageAlert(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //定义状态保存上一次温度值
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))


  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度
    val lastTemp = lastTempState.value()
    //跟最新的温度求差值作比较
    val diff = (value.temperature - lastTemp).abs
    if(diff > threshold)
      out.collect((value.id,lastTemp,value.temperature))
    //跟新温度值
    lastTempState.update(value.temperature)

  }
}
//KeyedState 测试：必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading,String]{

  var valueState:ValueState[Double] = _
  //lazy 模式
  lazy val listState:ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState",classOf[Int]))
  lazy val mapState:MapState[String,Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("mapState",classOf[String],classOf[Double]))
  lazy val reduceState:ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState",new MyMaxTemp,classOf[SensorReading]))
  override def open(parameters: Configuration): Unit = {
     valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState",classOf[Double]))
  }

  override def close(): Unit = super.close()

  override def map(value: SensorReading): String = {
    // 状态的读写
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()

    list.add(2)
    list.add(3)
    listState.addAll(list)
//    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("Sensor_1",12)

    reduceState.get()
    reduceState.add(value)
    value.id
  }
}

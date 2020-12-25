package com.bupt.apiTest.tableTest.udfTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @author yangkun
 * @date 2020/12/25 16:33
 * @version 1.0
 */
object TableAggregateFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 读取数据
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("input/sensor.txt")
    //    1. 对应processtime 处理时间
    //    val dataStream = inputStream
    //      .map(line => {
    //        val arr = line.split(",")
    //        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    //      })
    // 2. 事件时间
    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new timestamps.BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
      }
      )

    //1. 对应process time 处理时间
    //    val sensorTable = tableEnv.fromDataStream(dataStream,'id,'temperature,'timestamp,'pt.proctime)
    // 2. 对用eventtime  事件时间
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    //table api
    val top2Temp = new Top2Temp
    val resultTable = sensorTable
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    resultTable.toRetractStream[Row].print()
    env.execute("table aggregateFunction test")

  }

}

//定义一个类，用来表示聚合函数状态
class Top2TempAcc {
  var highestTemp: Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

//自定义表聚合函数，提取所有温度值中最高的两个温度 输出（temp，rank）
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  //实现计算聚合结果函数accumulate
  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    //判断当前温度值是否比状态中的大
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    }
    else if (temp > acc.secondHighestTemp && temp < acc.highestTemp) {
      acc.secondHighestTemp = temp
    }
  }

  //实现一个输出结果的方法，最终处理完表中所有数据的调用
  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}


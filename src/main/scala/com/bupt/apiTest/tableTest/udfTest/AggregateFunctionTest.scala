package com.bupt.apiTest.tableTest.udfTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @author yangkun
 * @date 2020/12/25 15:52
 * @version 1.0
 *          more to  one
 */
object AggregateFunctionTest {

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
    val avgTemp = new AvgTemp()
    val resultTable = sensorTable
      .groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)

    //sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, avgTemp(temperature)
        |from
        |sensor
        |group by id
        |""".stripMargin)

    resultSqlTable.toRetractStream[Row].print("sql")
    resultTable.toRetractStream[Row].print("result")
    env.execute("aggregate exec test")

  }

}

//定义一个类,专门用于表述聚合的状态
class AvgTempAcc {
  var sum: Double = 0.0
  var count: Int = 0
}

//自定义聚合函数, 求每个传感器的平均温度值,保存状态(tempSum,tempCount)
class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc) = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  //还要实现一个具体的处理计算函数，accumulate
  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
    accumulator.sum += temp
    accumulator.count += 1
  }
}

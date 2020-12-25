package com.bupt.apiTest.tableTest.udfTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @author yangkun
 * @date 2020/12/25 14:52
 * @version 1.0
 *          one to one
 */
object ScalarFunctionTest {
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

    //调用自定义hash函数,对id 进行hash运算
    // 1. table api
    // 首先new 一个UDF的实例
    val hashCode = new HashCode(123)
    val resultTable = sensorTable.select('id, 'ts, hashCode('id))

    //2. sql
    //需要在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashcode", hashCode)

    val resultSqlTable = tableEnv.sqlQuery("select id,ts,hashCode(id) from sensor")
    resultSqlTable.toAppendStream[Row].print("sql")
    resultTable.toAppendStream[Row].print("result")
    env.execute("scalar exec test")
  }

}

//自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}

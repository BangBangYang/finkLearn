package com.bupt.apiTest.tableTest.udfTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @author yangkun
 * @date 2020/12/25 15:08
 * @version 1.0
 *          one to more
 */
object TableFunctionTest {
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

    // 1. table api
    val split = new Split("_")
    val resultTable = sensorTable
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    //2. sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | ts,
        | word,
        | length
        |from
        | sensor,lateral table(split(id)) as splitId(word,length)
        |""".stripMargin)


    resultSqlTable.toAppendStream[Row].print("sql")
    resultTable.toAppendStream[Row].print("result")
    env.execute("table exec test")
  }
}

//自定义TableFunction
class Split(seperator: String) extends TableFunction[(String, Int)] {
  def eval(Str: String): Unit = {
    Str.split(seperator).foreach(
      word => collect((word, word.length))
    )
  }
}

package com.bupt.apiTest.tableTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author yangkun
 * @date 2020/12/23 10:37
 * @version 1.0
 */
object TimeAndWinsowTest {
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
    //    sensorTable.printSchema()
    //    sensorTable.toAppendStream[Row].print()

    //1 .group window
    //1.1 table api
    val resultTable = sensorTable
      //        .window(Tumble.over(10.seconds).on('ts).as('w)) //每10s 统计一次，滚动时间窗口
      //        .window(Tumble over(10.seconds) on('ts) as('tw)) //每10s 统计一次，滚动时间窗口 scala 另一种写法（单一参数传递省略" . "和"()" 直接用空格）
      .window(Tumble over 10.seconds on 'ts as 'tw) //每10s 统计一次，滚动时间窗口 scala 另一种写法（单一参数传递省略" . "和"()" 直接用空格）
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'temperature.avg, 'tw.end)
    //1.2. sql
    tableEnv.createTemporaryView("sensor", sensorTable) //注册一个表
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | avg(temperature),
        | tumble_end(ts,interval '10' second)
        | from sensor
        | group by
        |   id,
        |   tumble(ts,interval '10' second)
        |""".stripMargin)
    //转化流打印输出
    //    resultTable.toAppendStream[Row].print("result")
    //    resultSqlTable.toAppendStream[Row].print("sql")
    //2. over window :统计每个sensor每条数据，与之前两行的平均温度
    //2.1 table api
    val overResultTable = sensorTable
      .window(Over.partitionBy("id").orderBy("ts").preceding(2.rows).as("ow"))
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
    overResultTable.toAppendStream[Row].print("result")


    //2.2
    val overSQLTable = tableEnv.sqlQuery(
      """
        |select
        |id,
        |ts,
        |count(id) over ow,
        |avg(temperature) over ow
        |from sensor
        |window ow as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        | )
        |""".stripMargin)
    overSQLTable.toAppendStream[Row].print("sql")
    env.execute("time and window test")

  }
}

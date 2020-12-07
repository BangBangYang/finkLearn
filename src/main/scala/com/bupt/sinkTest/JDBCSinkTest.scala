package com.bupt.sinkTest


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.bupt.apiTest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author yangkun
 * @date 2020/12/2 14:50
 * @version 1.0
 */
object JDBCSinkTest {
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
    mapStream.print()
    //自定义
    mapStream.addSink(new MyJDBCSink)
    env.execute("JDBC Test")
  }
}
class MyJDBCSink extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hdp4.buptnsrc.com:3306/test","root","adminzhang123")
    insertStmt = conn.prepareStatement("insert into sensor_temp(id,temperature) values(?,?)")
  }

  override def close(): Unit = super.close()

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {

  }
}

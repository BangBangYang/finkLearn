package com.bupt.apiTest.tableTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * @author yangkun
 * @date 2020/12/20 15:33
 * @version 1.0
 */
object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 文件读取
    val stream: DataStream[String] = env.readTextFile("input/sensor.txt")
    //转化为样例类
    val dataStream = stream.map(line =>{
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
    })
    //首先创建表的执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于流创建表
    val dataTable:Table = tableEnv.fromDataStream(dataStream)
    //调用table api 进行转化
    val resultTable = dataTable.select("id,temperature")
        .filter("id == 'sensor_1'")
    //直接用sql实现
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sql = "select id,temperature from dataTable where id = 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)
    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("result_sql")
    //执行
    env.execute("source test")
  }

}

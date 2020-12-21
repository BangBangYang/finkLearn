package com.bupt.apiTest.tableTest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}


/**
 * @author yangkun
 * @date 2020/12/20 20:04
 * @version 1.0
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    //1. 表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    /*
      //1.1 老版本的流处理环境
        val settings = EnvironmentSettings.newInstance()
          .useOldPlanner()
          .inStreamingMode()
          .build()
        val oldStreamTableEnv = StreamTableEnvironment.create(env,settings)
        //1.2 老版本的批处理环境
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)
        //1.3 基于blink planner的 新的流处理环境
        val blinkStreamSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings)

        //1.4 新的批处理环境
        val blinkBatchSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inBatchMode()
          .build()
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
    */
    //2.1 从文件读取数据
    tableEnv.connect(new FileSystem().path("input/sensor.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    //2.2 从kafka读取数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("a")
      .property("bootstrap.servers", "hdp4.buptnsrc.com:6667")
      .property("zookeeper.connect", "hdp4.buptnsrc.com:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timeStamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaInputTable")
    val kafkaInputTable = tableEnv.from("kafkaInputTable")

//    val sensorTable = tableEnv.from("kafkaInputTable")
//    sensorTable.toAppendStream[(String, Long, Double)].print("kafkaInputTable")

    //3 查询转化
    //3.1 使用table api
    val sensorTable = tableEnv.from("inputTable")
    val resultTable =sensorTable.select('id,'temperature)
    sensorTable.select("id,temperature")
    .filter('id === "sensor_1")

    //3.2 使用table sql
    val resultSqlTable=tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin)
    resultTable.toAppendStream[(String,Double)].print("resultTable")
    resultSqlTable.toAppendStream[(String,Double)].print("resultSqlTable")
    env.execute("table api test")
  }

}

package com.bupt.apiTest.tableTest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author yangkun
 * @date 2020/12/21 17:13
 * @version 1.0
 */
object KafkaPiplineTest {
  def main(args: Array[String]): Unit = {
    //1. 表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    // 2.kafka读取数据
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
    //3 查询转换
    val kafkaInputTable = tableEnv.from("kafkaInputTable")

    //3.1 简单转化
    val resultTable = kafkaInputTable.select('id, 'temperature)
      .filter('id === "sensor_1")
    //    resultTable.toAppendStream[(String,Double)].print("res")
    //3.2 聚合转化
    val aggTable = kafkaInputTable
      .groupBy("id") //基于id分组
      .select('id, 'id.count as 'count)

    //4 输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("ads-yk")
      .property("bootstrap.servers", "hdp4.buptnsrc.com:6667")
      .property("zookeeper.connect", "hdp4.buptnsrc.com:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")
    resultTable.insertInto("kafkaOutputTable")
    env.execute("kafka pipline test")
  }
}

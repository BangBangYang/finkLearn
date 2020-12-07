package com.bupt.sinkTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author yangkun
 * @date 2020/12/1 20:35
 * @version 1.0
 */
object RedisSinkTest {
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
    // 写入redis
    // 定义jedis连接


  val config = new FlinkJedisPoolConfig.Builder()
    .setHost("10.108.113.211")
    .setPort(6379)
    .build()

    mapStream.addSink( new RedisSink[SensorReading]( config, new MyRedisMapper() ) )
    env.execute()
  }

}

class MyRedisMapper() extends RedisMapper[SensorReading]{
  // 写入redis的命令，保存成Hash表 hset 表名 field value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor")

  override def getValueFromData(data: SensorReading): String = data.temperature.toString

  override def getKeyFromData(data: SensorReading): String = data.id
}

package com.bupt.sinkTest

import com.bupt.apiTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @author yangkun
 * @date 2020/11/30 21:09
 * @version 1.0
 */
object FileSinkTest {
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
    //2.1 write csv
//    mapStream.writeAsCsv("input/out.txt")
    //2.2
    mapStream.addSink(
      StreamingFileSink.forRowFormat(
      new Path("input/out1.txt"),
        new SimpleStringEncoder[SensorReading]()
     ).build()
    )
    env.execute()
  }

}

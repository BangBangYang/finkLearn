package com.bupt.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @author yangkun
 * @date 2020/11/22 19:29
 * @version 1.0
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")
//    env.setParallelism(4)
    //接收一个socket文本流
    val inputDS: DataStream[String] = env.socketTextStream(host,port)
    //进行转化处理统计
    val flatDS: DataStream[String] = inputDS.flatMap(_.split(" "))
    //进行转化处理统计
    val res: DataStream[(String, Int)] = flatDS.map((_, 1))
      .keyBy(0)
      .sum(1)
    res.print().setParallelism(1)
    //启动任务执行
    env.execute("socket word count stream")
  }


}

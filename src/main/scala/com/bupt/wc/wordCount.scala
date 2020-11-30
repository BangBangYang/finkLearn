package com.bupt.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * @author yangkun
 * @date 2020/11/21 20:57
 * @version 1.0
 */
object wordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //文件中读取数据
    val inputDS = env.readTextFile("input/wc.txt")
    //对数据进行转化处理统计，先分词，再按照word进行分组，最后进行聚合统计
    val res = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)  //以第一个元素作为key，进行分组
      .sum(1)  //对所有数据的第二个元素求和
    //打印输出
    res.print()
  }

}

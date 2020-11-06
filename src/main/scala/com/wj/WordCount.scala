package com.wj

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}


/**
 *
 * 批处理的代码
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val filepath="D:\\ideaProject\\flink-demo\\src\\main\\resources\\wordcount.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile(filepath)
    val dataSet = value.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    dataSet.print()
  }
}

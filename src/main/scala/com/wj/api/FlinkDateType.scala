package com.wj.api

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


/**
 * flink 支持的基础数据类型
 */
object FlinkDateType {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val value1= env.fromElements(1, 2, 3, 4, 5, 6, 7).map(x => x + 1) //scala 数据类型
    value1.print()
    val value2 = env.fromElements(("admin",123),("boot",2312)) //scala 元组
    val res = value2.filter(p => p._2 < 100)
    env.execute()

  }

}

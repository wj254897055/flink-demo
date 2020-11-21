package com.wj.api.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object SourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = List("aaa", "bbb", "ccc")
    val list2 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val stream = env.fromCollection(list2)
    val s1 = stream.filter(_.%(2).!=(0)).map(("g", _))
    s1.print("source test").setParallelism(1)
    env.execute("source test")
  }

}

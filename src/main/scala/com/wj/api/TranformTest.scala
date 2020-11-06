package com.wj.api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * flinK 的 转换算子
 *
 */
object TranformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.execute("tranform job")
  }

}

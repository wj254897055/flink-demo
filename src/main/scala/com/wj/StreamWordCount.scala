package com.wj

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream = env.socketTextStream(host, port)
    val wordCountStream = textStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    wordCountStream.print().setParallelism(1)

    //提交任务
    env.execute("job name: stream word count ")
  }


}

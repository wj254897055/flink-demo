package com.wj.api

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


/**
 * flinK 的 转换算子
 *
 */
object TranformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局修改并行度
    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")
    //map
    val stream2 = stream.map(value => {
      val arrys = value.split(",")
      SensorReading(arrys(0).trim, arrys(1).trim.toLong, arrys(2).trim.toDouble)
    }).keyBy("id")

      //.sum(2)
    //输出上一次时间戳+1，上一次温度+10；

      .reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature+10))
    stream2.print()
    env.execute("tranform job")
  }

}

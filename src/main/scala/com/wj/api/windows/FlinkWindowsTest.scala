package com.wj.api.windows

import com.wj.api.source.SensorReading
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 * flink window test
 */
object FlinkWindowsTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })

    //每10秒 获取温度的最小值
    val windowDataStream=dataStream.map(date=>(date.id,date.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((d1,d2)=>(d1._1,d1._2.min(d2._2))) //使用reduce做增量数据



    env.execute("flink window test")


  }

}

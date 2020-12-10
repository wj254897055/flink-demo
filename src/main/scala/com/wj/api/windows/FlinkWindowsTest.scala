package com.wj.api.windows

import com.wj.api.source.SensorReading
import com.wj.api.watermark.{PeriodicAssigner, PunctuateAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100) //周期性生成waterMark


    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })

     // .assignAscendingTimestamps(_.timestamp*1000) //实时的
     // .assignTimestampsAndWatermarks(new PunctuateAssigner)
     .assignTimestampsAndWatermarks(new PeriodicAssigner)

    //高并发情况下，乱序的情况下指定watermark
    //    dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
    //    (Time.milliseconds(1000)) {  //延迟等待时间
    //      override def extractTimestamp(t: SensorReading): Long = {
    //      t.timestamp*1000
    //      }
    //    })

    //每10秒 获取温度的最小值
    val windowDataStream=dataStream.map(date=>(date.id,date.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((d1,d2)=>(d1._1,d1._2.min(d2._2))) //使用reduce做增量数据

    env.execute("flink window test")
  }

}

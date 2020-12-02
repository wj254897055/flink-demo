package com.wj.api.watermark

import com.wj.api.source.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark


/**
 * waterMark fLink
 */
object FlinkWaterMarkTest {


}

class PunctuateAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound:Long=60*1000

  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if (t.id=="sensor_1"){
      new Watermark(l-bound)
    }else{
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp
  }
}


//定义 周期性的检查
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

  val bound:Long=60*1000
  var maxTs: Long = Long.MaxValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs-bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs=maxTs.max(t.timestamp)
    t.timestamp
  }
}


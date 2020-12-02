package com.wj.api.sink

import java.util

import com.wj.api.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests



/**
 * flink es sink
 */
object FlinkElasticSearchSink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })




    //高并发情况下，乱序的情况下指定watermark
//    dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
//    (Time.milliseconds(1000)) {  //延迟等待时间
//      override def extractTimestamp(t: SensorReading): Long = {
//      t.timestamp*1000
//      }
//    })


    //对于排好序的数据，不需要延迟触发
//    dataStream.assignAscendingTimestamps(_.timestamp*1000)

    val httpHost=new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("192.168.80.100",9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHost,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data " + t)
          val hashMap = new util.HashMap[String, String]()
          hashMap.put("senor_id", t.id)
          hashMap.put("temperature", t.temperature.toString)
          hashMap.put("timestamp", t.timestamp.toString)
          //将封装好的对象准备发送数据
          val request = Requests.indexRequest
            .index("sensor_reading")
            .`type`("sensor_reading")
            .source(hashMap)
          requestIndexer.add(request)
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")

  }
}

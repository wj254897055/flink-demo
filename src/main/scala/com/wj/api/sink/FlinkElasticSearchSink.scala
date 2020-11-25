package com.wj.api.sink

import java.util

import com.wj.api.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionRequest
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

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })


    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")

  }
}

package com.wj.api.sink

import java.util.Properties

import com.wj.api.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * flink kafka sink
 */
object FlinkKafkaSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers","192.168.80.100:9092")
    prop.setProperty("group.id","consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    //flink 加载kafka
    val kf = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop))
    val dataStream = kf
      .map(
        data => {
          val dataArray = data.split(",")
          val v1=dataArray(2)
          val v2=dataArray(2).trim
          val v3=dataArray(2).trim.toDouble
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString  // 转成String方便序列化输出
        }
      )
    //sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("sink-test",new SimpleStringSchema(),prop))
    dataStream.print()

    env.execute("kafka sink job")
  }

}

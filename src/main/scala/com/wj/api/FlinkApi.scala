package com.wj.api



import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumer09}


//温度传感器 样例类
case class SensorReading( id:String,timestamp:Long,temperature:Double)


object FlinkApi {
  def main(args: Array[String]): Unit = {

    //Environment 环境相关的api
    //创建流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val env2 = StreamExecutionEnvironment.createLocalEnvironment(2)
//    val env3 = StreamExecutionEnvironment.createRemoteEnvironment("host",6139,"jar")

    //source
    //从kafka 中读取数据
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","192.168.80.100:9092")
    prop.setProperty("group.id","consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    //flink 加载kafka
    val kf = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop))

    kf.print("stream-kafka").setParallelism(1)

    //从自定义的集合中获取数据
//    val stream1 = env.fromCollection(List(
//      SensorReading("senor_1", 1547718199, 23.45354),
//      SensorReading("senor_2", 1547728199, 24.124533),
//      SensorReading("senor_3", 1547738199, 25.125753),
//      SensorReading("senor_4", 1547748199, 26.17423),
//      SensorReading("senor_5", 1547758199, 28.1646423)
//    ))
//
//    stream1.print("stream1")

    env.execute()




  }

}

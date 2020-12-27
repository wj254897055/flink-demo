package com.wj.api.sink

import com.wj.api.entity.VehicleOrigInfoEntity
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.json4s.jackson.JsonMethods.parse

import java.util.Properties

object QsdiKafkaSink extends App {

  // 构建flink 环境的
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)


  //kafka 配置
  val prop = new Properties()
  prop.setProperty("bootstrap.servers","192.168.1.121:9092")
  prop.setProperty("group.id","qsdi-consumer-group-flink-vehicle")
  prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("auto.offset.reset", "latest")


  //flink 加载kafka
  val kf = env.addSource(new FlinkKafkaConsumer011[String]("UNVIEW-MOTOR_VEHICLE", new SimpleStringSchema(), prop))


  val dataStream = kf
    .map(
      data => {
        println(data)
        val vehicles = parse(data)
        val options = vehicles.toOption
        VehicleOrigInfoEntity
      }
    )


  env.execute("qsdi flink job")

}

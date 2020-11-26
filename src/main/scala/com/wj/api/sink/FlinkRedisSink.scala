package com.wj.api.sink

import com.wj.api.source.SensorReading
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 * flink redis sink
 */
object FlinkRedisSink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })

    //redis sink
    val builder = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.80.100")
      .setPort(6379)
      .build()
    dataStream.addSink(new RedisSink(builder,new MyRedisMapper))

    env.execute("redis sink test")

  }

}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  override def getCommandDescription: RedisCommandDescription = {
    //use redis hset
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }
  override def getKeyFromData(t: SensorReading): String = {
    println("sensor-->"+t)
    t.id
  }
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }

}

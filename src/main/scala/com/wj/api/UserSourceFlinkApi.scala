package com.wj.api

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import scala.util.Random


/**
 * 自定义source
 */


object UserSourceFlinkApi {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //自己去实现
    val stream = env.addSource(new SensorSource())
    stream.print("stream4").setParallelism(1)
    env.execute("source test")

  }

}

 class SensorSource() extends SourceFunction[SensorReading]{

   //定义一个flag  表示数据源是否正常运行
   var running:Boolean =true

   //正常生成数据
   override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
     //初始化随机数生成器
     val random = new Random()

     //初始化定义一组传感器温度
     var curTemp=1.to(10).map(
       i=>("sensor_"+i,60+random.nextGaussian())
     )
     while (running){

       //更新数据
       curTemp=1.to(10).map(
         i=>("sensor_"+i,60+random.nextGaussian())
       )
       //获取当前时间戳
       val timestamps = System.currentTimeMillis()
       curTemp.foreach(
         t=>sourceContext.collect(SensorReading(t._1,timestamps,t._2)) //发送数据
       )
       //设置时间间隔 1s
       Thread.sleep(1000)
     }

   }

   //表示取消数据源的生成
   override def cancel(): Unit = {
     running=false
   }
 }

package com.wj.api.processFunction

import com.wj.api.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * flink my ProcessFunction
 */

case class MyProcess() extends KeyedProcessFunction[String,SensorReading,String] {
  //定义一个状态 用来保存上一个数据的温度值
  lazy val last_temp:ValueState[Double]=getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("last_temp",classOf[Double]))

  //定义一个状态 保存定时器的时间
  lazy val currentTimer:ValueState[Long]=getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("currentTs",classOf[Double]))


  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp=last_temp.value()
    //更新温度值
    last_temp.update(value.temperature)

    val currentTimers = currentTimer.value()

    //温度连续上升 则注册定时器 且没有注册定时器
    if (value.temperature>preTemp && currentTimers==0){
      val timeTs=ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(timeTs+1000)
      currentTimer.update(timeTs)
    }else if (value.temperature<preTemp || preTemp==0){
      //删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(currentTimers)
      currentTimer.clear()
    }

//
//    ctx.timerService()
//      //.registerProcessingTimeTimer() 机器时间
//      //.currentWatermark() //获取当前的事件时间
//      .registerEventTimeTimer(1000) //事件时间
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey+" 温度连续上升 ")
    currentTimer.clear()
  }
}

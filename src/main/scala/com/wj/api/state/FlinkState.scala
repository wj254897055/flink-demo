package com.wj.api.state

import org.apache.flink.api.scala._
import com.wj.api.source.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * flink的状态：一个任务来维护，计算某个结果的所有数据，状态就是一个本地变量，可以被任务访问到
 * 算子状态 operation state ===>数据结构：
 *         列表状态（list state）、
 *         联合列表状态(union list state)、
 *         广播状态( )
 * 键控状态 keyed state 输入流中定义的key来维护
 *        键控状态的数据结构：值状态 value state 存为值
 *                          列表状态 list state 存为列表
 *                          映射状态 map state key-value
 *                          聚合状态 reducing state && aggregating state
 * 状态后端 state backends 每传一条数据有状态的的算子任务都会读取和更新状态
 *        状态存储访问和维护，由一个可以插入的组件决定。主要负责两件事情，本地状态的管理，以及检查点的状态远程存储
 *         1：MemoryStateBackend 内存级的状态后端，会将键控状态作为内存中的对象进行管理 taskManager 的 jvm 堆上，而将
 *         checkpoint 存储在jobmnager的内存中
 *         2：fsStateBackend 把checkpoint存在远程的持久化文件系统，而对于本地状态存储在taskManager堆上
 *
 */
object FlinkState {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000) //隔1秒checkpoint\


    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      } )

//    val processedStream = dataStream
//      .process( new FreezingAlert() )

    //连续两个温度跳变
    val processedStream = dataStream.keyBy(_.id)
//        .process(new TempChangeAlert(10.0))
        .flatMap(new TempChangeAlert2(10.0))

    val processedStream2=dataStream.keyBy(_.id)
        .flatMapWithState[(String,Double,Double),Double]{
              //如果没有状态，也就是没有数据过来，那么就将当前的状态置为空
          case (input:SensorReading,None)=>(List.empty,Some(input.temperature))
            //如果有状态，就和上次的状态值比较
          case (input:SensorReading,lastTemp:Some[Double])=>{
            val diff =(input.temperature-lastTemp.get).abs
            if (diff>10){
              (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
            }else{
              (List.empty,Some(input.temperature))
            }
          }
        }



    //    dataStream.print("input data")
//    processedStream.print("processed data")
//    processedStream.getSideOutput( new OutputTag[String]("freezing alert") ).print("alert data")


    env.execute("side output test")
  }

}

case class TempChangeAlert(threshold:Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)] {
  //定义一个状态变量 保持上次的温度值
  lazy val lastTemState:ValueState[Double]=getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  override def processElement(value: SensorReading,
                              context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
                              collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val last = lastTemState.value()
    //拿当前的温度值和上次的温度值进行比较，如果大于阈值就报警
    val diff = (value.temperature - last).abs
    if (diff>threshold){
      collector.collect((value.id,last,value.temperature))
    }
    lastTemState.update(value.temperature)
  }
}

case class TempChangeAlert2(threshold:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)] {

  private var lastTemState:ValueState[Double]=_

  override def open(parameters: Configuration): Unit = {
    lastTemState=getRuntimeContext
      .getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  }

  override def flatMap(value:SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val last = lastTemState.value()
    //拿当前的温度值和上次的温度值进行比较，如果大于阈值就报警
    val diff = (value.temperature - last).abs
    if (diff>threshold){
      collector.collect((value.id,last,value.temperature))
    }
    lastTemState.update(value.temperature)
  }


}

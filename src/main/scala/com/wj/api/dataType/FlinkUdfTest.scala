package com.wj.api.dataType

import com.wj.api.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * flink 自定义函数类 UDF函数
 */
object FlinkUdfTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局修改并行度
    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")
    val splitStream =stream.map( values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })
    //匿名函数类
    val value1 = splitStream.filter(_.id.startsWith("senor_1"))
    //val value1 = splitStream.filter(data=>data.id.startsWith("senor_1"))
    //自定义函数方式
    val value2 = splitStream.filter(new Myfilter())

    value1.print()
    value2.print()
    env.execute(" udf streaming test")
  }



}

class Myfilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
  t.id.startsWith("senor_2")
  }
}


class MyMapper() extends RichMapFunction[SensorReading,String]{
  var subtask=0
  override def map(in: SensorReading): String = {
    in.temperature.toString
  }

  override def open(parameters: Configuration): Unit = {
    subtask = getRuntimeContext.getIndexOfThisSubtask
    //以下可以做hdfs 连接
  }

  override def close(): Unit = super.close()
}

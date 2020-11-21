package com.wj.api

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


/**
 * flinK 的 转换算子
 *
 */
object TranformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局修改并行度
    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")





    //map
    val stream2 = stream.map(value => {
      val arrys = value.split(",")
      SensorReading(arrys(0).trim, arrys(1).trim.toLong, arrys(2).trim.toDouble)
    }).keyBy("id")
    //.sum(2)
    //输出上一次时间戳+1，上一次温度+10；
        .reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature+10))
//        stream2.print()


      val splitStream =stream.map( values=>{
        val arrays= values.split(",")
        SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
      }).split(data=>{
        if (data.temperature>30)Seq("high") else Seq("low")
      })

    val high=splitStream.select("high")
    val low=splitStream.select("low")
    val all=splitStream.select("high","low")
//    high.print("high")
//    low.print("low")
//    all.print("all")

    //使用union 合并多条流
    val unionStream = high.union(low)
    unionStream.print("union")


    //合并两条流
    val warning = high.map(data => (data.id, data.temperature))

    val coMapStreaming = warning.connect(low)
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowData => (lowData.id, "healthy")
      )

//    coMapStreaming.print("coapStreaming")


    env.execute("tranform job")
  }

}

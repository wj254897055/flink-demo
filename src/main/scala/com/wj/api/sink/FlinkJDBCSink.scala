package com.wj.api.sink

import java.sql
import java.sql.DriverManager

import com.mysql.jdbc.{Connection, PreparedStatement}
import com.wj.api.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * flink jdbc
 */
object FlinkJDBCSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.readTextFile("D:\\ideaProject\\flink-demo\\src\\main\\resources\\sensor.txt")

    //transform
    val dataStream =stream.map(values=>{
      val arrays= values.split(",")
      SensorReading(arrays(0).trim, arrays(1).trim.toLong, arrays(2).trim.toDouble)
    })

    dataStream.addSink(new MyJdbcSink())

    env.execute(" jdbc sink")

  }
}
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义sql连接,预编译s
  var conn:sql.Connection=_
  var insertStmt:sql.PreparedStatement=_
  var updateStmt:sql.PreparedStatement=_

  //初始化 创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn=DriverManager.getConnection("jdbc:mysql://192.168.80.100:3306/test","root","123456")
    insertStmt=conn.prepareStatement("insert  into temperature (seneot,temp) values (?,?)")
    updateStmt=conn.prepareStatement("update temperature set temp =? where  seneot=?")
  }

  //调用连接 执行语句
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit ={
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)

    updateStmt.execute();
    //如果update 没有查到数据，那么执行插入
    if(updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  //关闭数据连接
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}


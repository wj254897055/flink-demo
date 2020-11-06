package com.wj

import scala.collection.mutable.ArrayBuffer


case class trackLog(id:Int,name:String,logContent:String)
object ScalaStudy {

  def main(args: Array[String]): Unit = {

    var arr1 = ArrayBuffer(1,2,3,4,5,6,7,8,9,10)

    val f1 = arr1.map(_=>{

    })
    println(f1)

////    使用filter过滤器,过滤出来偶数
//    println(arr1.filter(_ % 2 == 0))


    //3、请写出for循环，i 表示循环的变量，Range生成0-20的数字，请循环打印出0-20内的偶数不包括20。
//    for (i<-Range(0,21)){
//      if (i%2==0&&i!=20)println(i)
//    }


    //2、请写出for循环，i 表示循环的变量，Range生成0-20的数字，请循环打印出0-20内的奇数。
//
//    for (i<-Range(0,21)){
//      if (i%2!=0)println(i)
//    }


    //1、请写出for循环，i 表示循环的变量，Range生成0-20的数字，请循环打印出0-19这些数字
//        for (i<- Range(0,21)){
//          breakable{
//            if (i==20)break()
//            println(i)
//          }
//        }




  }

 val getMyname = (name:String)=>print(name)

}

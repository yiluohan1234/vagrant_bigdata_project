package com.cuiyf41.spark.core.rdd.serial

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Serial {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List[Int]())

    val user = new User()

    // SparkException: Task not serializable
    // NotSerializableException: com.cuiyf41.spark.core.rdd.serial.Operator_Action$User

    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()

  }
  //class User extends Serializable {
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  //case class User() {
  class User {
    var age : Int = 30
  }
}
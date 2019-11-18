package com.dolen.spark.realtime

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* spark订单信息，时时计算
* */
object OrderApp {

  def main(args: Array[String]): Unit = {
//
    val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[3]");//本地调试，
//
    val sc: Any = new StreamingContext(sparkConf,Seconds(5))








  }
}

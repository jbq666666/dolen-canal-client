package com.dolen.spark.realtime

import com.alibaba.fastjson.JSON

case class User(id: Long,organization_id: Long,username: String ,role_ids: String,locked: Boolean)




object testJson01{

  def main(args: Array[String]): Unit = {

    val jsonstr = "{\"id\":1,\"organization_id\":1,\"username\":\"admin\",\"role_ids\":\"1,2,3,4\",\"locked\":false}"

    println(JSON.parseObject(jsonstr, classOf[User]))

  }



}

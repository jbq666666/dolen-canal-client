package com.dolen.spark.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.dolen.spark.realtime.util.{JedisConnectionPool, MykafkaUtil}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object TestDAU {

    def main(args: Array[String]): Unit = {

        //只要在要运行的代码(程序入口)内加入下面两行代码即可.
//        LoggerLevels.setStreamingLogLevels()
//        Logger.getLogger("org").setLevel(Level.ERROR)
    //    Logger.getLogger("org").setLevel(Level.WARN)
//        Logger.getLogger("kafka").setLevel(Level.WARN)
   //     Logger.getRootLogger.setLevel(Level.WARN)

        //1.
        val sparkConf = new SparkConf().setAppName("TestDAU").setMaster("local")
          .set("spark.streaming.kafka.maxRatePerPartition", "3000")
          .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        //2.
        val sc = new StreamingContext(sparkConf, Seconds(5));

        //3.ConsumerRecord[String, String]  k,v
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("test", sc);

        //4.打印结果
        inputDstream.foreachRDD { rdd =>
            println(rdd.map(_.value()).collect().mkString("\n"));
        }

        // 5.统计日活：
        //5.1 转换 结构变成case class 补充两个时间字段
        val startupLogDstream: DStream[StartUpLog] = inputDstream.map
            { record =>
                val startupJsonString :String = record.value();
                //println("+++++++++++++++++"+ startupJsonString)

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)

                //将map转换成bean对象
                val startuplog :StartUpLog = mapper.readValue(startupJsonString, classOf[StartUpLog]);
                //println(mapper.readValue(startupJsonString, classOf[StartUpLog]).toString)

                //获取bean(此方法不通，暂时没到原因)
               //val startuplog: StartUpLog = JSON.parseObject(startupJsonString, classOf[StartUpLog])

                //新建一个日期
                println("111111"+new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts)))
                println("222222"+new SimpleDateFormat("yyyy-MM-dd HH").format(new Date()));

                val datetimesString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts))

                startuplog.logDate = datetimesString.split(" ")(0); //切分日期和小时
                startuplog.logHour = datetimesString.split(" ")(1)
                startuplog
            }

        //startupLogDstream.print(3)


        //5.2.2 利用清单进行过滤 去重
        val jedis: Jedis =JedisConnectionPool.getContion();
        val dauKey ="dau:"+ new SimpleDateFormat("yyyy-MM-dd").format(new Date());




        //5.2 去重复:mid 记录每天访问过的mid，形成一个清单
        // 5.2.1 更新清单，存储到redis中
        // 利用清单进行过滤去重： redis数据类型：string（好分片）、list、set、zet、hash
        startupLogDstream.foreachRDD{rdd=>
          rdd.foreachPartition { startuplogItr =>
            val jedis: Jedis = JedisConnectionPool.getContion()
            jedis;

              for (startuplog <- startuplogItr) {
                  val dauKey = "dau:" + startuplog.logDate;
                  println(dauKey+ "== "+startuplog.mid)
                  jedis.sadd(dauKey, startuplog.mid); //加入当天和mid
              }
              jedis.close(); //关闭
          }
        }







        print("启动程序....")
        sc.start();
        sc.awaitTermination();

    }




}

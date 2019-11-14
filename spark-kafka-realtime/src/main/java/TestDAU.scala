import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{LoggerLevels, MykafkaUtil}



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
        //2.
        val sc = new StreamingContext(sparkConf, Seconds(5));
        //3.ConsumerRecord[String, String]  k,v

        //sc.sparkContext.setLogLevel("WARN")



        val inputDstram: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("test", sc);





        //4.打印结果
        inputDstram.foreachRDD { rdd =>
            println(rdd.map(_.value()).collect().mkString("\n"));
        }

        print("启动程序....")
        sc.start();
        sc.awaitTermination();

    }

}

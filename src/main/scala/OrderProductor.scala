
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import scala.util.Random

/**
  * Created by zengxiaosen on 16/9/26.
  */
/*
在命令行输入 kafka-console-consumer.sh --zookeeper slave1:2181 --topic orderTopic
来看看我们生产的数据
 */
object OrderProductor {

  def main(args: Array[String]): Unit = {

    val topic = "orderTopic"
    val brokers = "master:9092,slave1:9092"
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    //生产10条订单
    while(true){
      (1 to 10).foreach{
        messageNum =>
          //地区ID,订单id,订单金额,订单时间
          val str = messageNum+","+Random.nextInt(10)+","+Math.round(Random.nextDouble()*100)+","+DateUtils.getCurrentDateTime
          val message = new ProducerRecord[String, String](topic, null, str)
          producer.send(message)

      }
      Thread.sleep(1000)
    }
  }

}

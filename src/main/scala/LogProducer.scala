import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import scala.util.Random

/**
  * Created by zengxiaosen on 16/9/26.
  */
object LogProducer {

  def main(args: Array[String]): Unit = {
    val topic = "logTopic"
    val brokers = "master:9092,slaves:9092"
    val messagePerSec = 10
    val wordsPerMessage = 10

    //zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    //1s 生产10行
    while(true){
      (1 to 10).foreach {
        messageNum =>
          //time, guid, url
          val str = DateUtils01.getCurrentTime()+",id_"+Random.nextInt(100)+","+"www.yhd.com/aa"+messageNum

          val message = new ProducerRecord[String, String](topic, null, str)
          producer.send(message)
      }
      Thread.sleep(1000)
    }

  }

}

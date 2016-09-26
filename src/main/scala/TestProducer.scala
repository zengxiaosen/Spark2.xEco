
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._




/**
  * Created by zengxiaosen on 16/9/26.
  */
object TestProducer {

  def main(args: Array[String]): Unit = {
    val topic = "logTopic"
    val brokers = "master:9092,slave1:9092"
    val messagesPerSec = 10 //每秒生产10个message
    val wordsPerMessage = 10 //每个message10个word

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    while(true){
      (1 to messagesPerSec.toInt).foreach{
        messageNum =>
          val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
            .mkString(" ")
          val message = new ProducerRecord[String, String](topic,null,str)
          producer.send(message)


      }
      Thread.sleep(1000)
    }


  }

}

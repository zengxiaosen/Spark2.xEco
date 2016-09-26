import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zengxiaosen on 16/9/26.
  * 这个程序运行时记得把检查点清空
  * 也就是执行:hadoop fs -rmr /user/root/checkpoint/uv/
  * 因为这个检查点之前消费的数据消费的烈数不相同
  */
object UV {

  /*
  uv:count(distinct guid) group by date
  借助set完成去重复
  检查点存全部数据
   */

  def main(args: Array[String]): Unit = {



    val checkpointDirectory = "hdfs://192.168.75.130:8020/user/root/checkpoint/uv"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        val topics = "logTopic"
        val brokers = "master:9092,slave1:9092"

        val sparkConf = new SparkConf().setAppName("uv").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint(checkpointDirectory)

        //create direct kafka stream with brokers and topics
        /*
        直接消费是不经过zookeeper的,所以这时候你就要告诉我kafka的地址,而不是zookeeper的地址里
         */
        val topicSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        //stringdecoder表示传的消息是字符串类型的
        /*
        不经过zk直接消费kafka,而通常情况下还是会经过zookeeper
        因为经过zookeeper你直接告诉zookeeper的地址就行了
        但是如果你用direct就要告诉它broker的地址了
        经过前人大量实验,直接消费的稳定性会好一些
        但是从框架的结偶度来讲,经过zookeeper的耦合度会低一些
         */
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicSet
        )

        computeUV(messages)

        ssc
      })
  }



  def computeUV(messages: InputDStream[(String, String)]) = {
    /*
    这个message里面是tuple2,第二列是数据
     */
    val sourceLog = messages.map(_._2)

    val utmUvLog = sourceLog.filter(_.split(",").size==3).map(logInfo => {
      val arr = logInfo.split(",")
      val date = arr(0).substring(0,10)
      val guid = arr(1)
      (date, Set(guid))
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    /*
    通过updatestatebykey进行聚合,聚合之后按照updateUvUtmCountState函数进行处理
     */
    val utmDayActive = utmUvLog.updateStateByKey(updateUvUtmCountState).//返回date,set(guid)
      //随意第一列result._1就是date,而result._2.size就是uv
      map(result =>{
      (result._1, result._2.size.toLong)
    }).print()

  }

  def updateUvUtmCountState(values:Seq[Set[String]], state:Option[Set[String]]) : Option[Set[String]] = {
    /*
    因为上一步是updatestatebykey,传进来的是value而没有key
    所以传进来的是Set(guid)
     */
    val defaultState = Set[String]()
    values match {
        //val Nil:scala.collection.immutable.Nil,它是一种特殊的类型,理解为空就行了
      //如果为空(首个批次)就返回一个空的state
      case Nil => Some(state.getOrElse(defaultState))
      //否则
      case _ =>
        //set与set拼接用++
        /*
        把seq里面所有的set拼接起来,它会自动完成去重
        set里面的元素自动进行去从,通过hashcode
         */
        val guidSet = values.reduce(_ ++ _)
        /*
        some其实就是option类型,返回的是一个包含全部guid的集合
         */
        println("11111-"+state.getOrElse(defaultState).size)
        println("22222-"+guidSet.size)
        Some(defaultState ++ guidSet)

    }
  }



}

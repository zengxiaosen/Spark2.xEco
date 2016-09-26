import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by zengxiaosen on 16/9/26.
  */
object AreaAntByWindow {

  def main(args: Array[String]): Unit = {
    val zkQuorum = "slave1:2181"
    val group = "g1"
    val topics = "logTopic"
    val numThreads = 2
    //setmaster的核数至少给2,如果给1,资源不够则无法计算,至少需要一个核进行维护,一个计算
    val sparkConf = new SparkConf().setAppName("direct").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))//两秒一个批次
    ssc.checkpoint("hdfs://192:168.75.130:8020/user/root/checkpoint/AreaAmt")//设置有状态检查点
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //val topicMap2 = Map(topics->2)
    //得出写到kafka里面每一行每一行的数据
    //每个时间段批次
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //产生我们需要的pair rdd
    val linerdd = lines.map{row =>{
      val arr = row.split(",")
      //按日期按地区计算销售额2016-09-04_Area
      /*
      继续细分到城市,无非是key该表一下,其他地方都是一样的
       */
      val key = arr(3).substring(0,10)+"_"+arr(0)
      val amt = arr(2).toInt
      (key, amt)
    }}.reduceByKeyAndWindow(
      _ + _,//加上新进入窗口的批次中的元素
      _ - _,//移除离开窗口的老批次中的元素
      Seconds(10),//窗口时长
      Seconds(2),//滑动步长
      2
    )
    linerdd.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created by zengxiaosen on 16/9/26.
  */
object AreaAmt {
  //每批次的wordcount

  def main(args: Array[String]): Unit = {
    /*
    对kafka来讲,groupid的作用是:
    我们想多个作业同时消费同一个topic时,
    1每个作业拿到完整数据,计算互不干扰;
    2每个作业拿到一部分数据,相当于实现负载均衡
    当多个作业groupid相同时,属于2
    否则属于情况1
     */
    val zkQuorum = "slave1:2181"
    val group = "g1"
    val topics = "logTopic"
    val numThreads = 2
    //setmaster的核数至少给2,如果给1,资源不够则无法计算,至少需要一个核进行维护,一个计算
    val sparkConf = new SparkConf().setAppName("AreaAmt").setMaster("local[2]")
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
    }}

    //initial state rdd for mapwithstate operation
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    /*
    word代表的是key
    one代表当前批次
    state代表以前批次
     */

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
    //mapwithstate是2.0里面的,但是没有updatestatebykey名气大
    val stateDstream = linerdd.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()



    val addFunc = (currValues: Seq[Int], preValueState: Option[Int]) =>{
      //通过spark内部的reducebykey按key规约,然后这里传入某key当前批次的seq,再计算key的总和
      val currentCount = currValues.sum
      //已经累加的值
      val previousCount = preValueState.getOrElse(0)
      //返回累加后的结果,是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    linerdd.updateStateByKey[Int](addFunc).print()








    ssc.start()
    ssc.awaitTermination()

  }

}

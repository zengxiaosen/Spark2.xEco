import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer.HashPartition
import org.apache.hadoop.mapred.lib
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
/**
  * Created by zengxiaosen on 16/9/23.
  */
object PartitionVisitCount {

  /*
  大表小表关联
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("useUDF").setMaster("local")
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext

    val fileRDD = sc.textFile("/opt/tarballs/spark_kafka/beifengspark/src/main/scala/2015082818")
      .filter(line=>line.length>0)
      .map{
        line =>
          val arr = line.split("\t")
          val date = arr(17).substring(0,10)
          val guid = arr(5)
          val url = arr(1)
          val uid = arr(18)
          (uid,(guid,url)) //key-value:tuple2
      }.partitionBy(new HashPartitioner(10)) //采用了hashcode分片方式,分成了10份,十个分区,每个分区10分
      /*
      相同的key在同一个分区,在进行任务调用时候,大表不需要任何shuffle
      只需要shuffle小表
       */
      .persist(StorageLevel.DISK_ONLY)


    /*
    parallelize有两个参数,第一个是他的list,第二个是分区数
    分区数可以不给,不给的情况下默认就是它的核数
     */
    //比如里面存的是我的用户id
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7),10)
      .map(i => (i+"", i+"str"))

    fileRDD.join(rdd).foreach(println)
    /*
    如果fileRDD后面还会关联好多个其他的rdd1,rdd2。。。rddn
    就要先把大的fileRDD进行分区
    这样优化了网络传输

     */


  }

}

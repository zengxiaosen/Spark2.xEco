import java.util.regex.{Matcher, Pattern}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by zengxiaosen on 16/9/23.
  */
object UseUDF {

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
          (date,guid,url)
      }.filter(i=>i._3.length>0).persist(StorageLevel.DISK_ONLY)

    //开发一个udf
    /*
    udf在hive中用得多,在spark中用的不多,原因就是其他地方也想用这个udf
    的话没办法传给它,没办法共享,而在hive中很方便,有初始化文件
    hive的udf都放在-i的初始化文件里
     */
    ss.udf.register("getName",(url: String,regex:String) =>{
      val p: Pattern = Pattern.compile(regex)
      val m: Matcher = p.matcher(url)
      if (m.find) {
         m.group(0).split("/")(1).toLowerCase()

      }else null


    })
    import ss.implicits._
    import ss.sql
    //每个活动页带来的流量
    fileRDD.toDF("date","guid","url").createOrReplaceTempView("log03")
    val sql03 =
      s"""
         |select getName(url,'sale/[a-zA-Z0-9]+'),count(distinct guid),
         |count(url) from log03 where url like '%sale%' group by
         |date,getName(url,'sale/[a-zA-Z0-9]+')
       """.stripMargin

    sql(sql03).rdd.foreach(println)

    sc.stop()
    ss.stop()

  }

}

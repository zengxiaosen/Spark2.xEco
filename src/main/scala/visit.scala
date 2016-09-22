import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by zengxiaosen on 16/9/20.
  */
object visit {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("visitCount").setMaster("local")
    val ss = SparkSession.builder().config(sparkconf).getOrCreate()
    val sc = ss.sparkContext
    import ss.implicits._

    val fileRDD = sc.textFile("/opt/tarballs/spark_kafka/beifengspark/src/main/scala/2015082818")
      .filter(line => line.length>0)
      .map{ line =>
        val arr = line.split("\t")
        val date = arr(17).substring(0, 10)
        val guid = arr(5)
        val sessionid = arr(10)
        val url = arr(1)
        (date,guid,sessionid,url)
        //通过url过滤
      }.filter(i => i._4.length>0).toDF("date","guid","sessionid","url")
      .persist(StorageLevel.DISK_ONLY)

    fileRDD.createOrReplaceTempView("log")
    /*
    guid是独立访客id,大于sessionid
     */
    val sql =
      s"""
         |select date,count(distinct guid) uv,sum(pv) pv,
         |count(case when pv>=2 then sessionid else null end) second_num,
         |count(sessionid) visits from
         |(select date, sessionid, max(guid) guid, count(url) pv from log
         |group by date,sessionid) a
         |group by date
       """.stripMargin

    val sql01 =
      s"""
         |select date,count(distinct guid) uv, count(url) pv from log
         |group by date
       """.stripMargin

    val result = ss.sql(sql).cache()
    result.show()
    result.printSchema()

    sc.stop()
    ss.stop()
  }

}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zengxiaosen on 16/9/23.
  */
object operJson_and_parquet {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("operJsonAndParquet").setMaster("local")
    val ss = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val sc = ss.sparkContext
    import ss.implicits._
    //读文本文件,生成普通rdd,可以通过toDF转化为dataframe,进而使用sql
    val fileRDD = sc.textFile("/opt/tarballs/spark_kafka/beifengspark/src/main/scala/2015082818")
    ss.read.json("/opt/tarballs/spark_kafka/beifengspark/src/main/scala/people.json")
      .createOrReplaceTempView("people")
    val rs = ss.sql("select * from people")
    rs.printSchema()
    rs.show()
    ss.read.parquet("/opt/tarballs/spark_kafka/beifengspark/src/main/scala/users.parquet")
      .createOrReplaceTempView("users")
    val rs2 = ss.sql("select * from users")
    rs2.printSchema()
    rs.show()

    sc.stop()
    ss.stop()

  }

}

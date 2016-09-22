import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zengxiaosen on 16/9/23.
  */
object hiveoperation {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hive").setMaster("local")
    val ss = SparkSession.builder()
        .enableHiveSupport()
        .config(sparkConf)
        .getOrCreate()
    import ss.implicits._

    val date = "2015-08-28" //通常通过参数传过来

    /*
    在hive表中
    desc track_log
    出现:
    id string
    url string
    referer string
    keyword string
    type string
    guid string
    pageid string
    moduleid string
    linkid string
    attachedinfo string
    sessionid string
    trackeru string
    cookie string
    ordercode string
    tracktime string
    enduserid string
    firstlink string
    sessionviewno string
    productid string
    curmerchantid string
    provinceid string
    cityid string
    ds string
    hour string

    #Partition information
    #col_name data_type comment
    ds        string
    hour      string
     */

    val sqlStr =
      s"""
         |insert overwrite into daily_visit partition (date='$date')    //日期,通常是通过参数传进来的
         |select date,count(distinct guid) uv,sum(pv) pv,
         |count(case when pv>=2 then sessionid else null end) second_num,
         |count(sessionid) visits from
         |(select ds date, sessionid, max(guid) guid, count(url) pv from tracklog and hour='18'
         |group by ds,sessionid) a
         |group by date
       """.stripMargin

    println("执行中。。。"+sqlStr)

    //返回dataframe,即dataset
    //val rdd = ss.sql(sqlStr)
    //rdd.rdd.foreach(println)

    /*
    把hive-site.xml放到工程里,执行无法直接连上hive!!!
    spark通过thrift服务,访问hive元数据库
    在pom中加入
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://master:9083</value>
    </property>
    在idea上从来没有实现过,所以还是打包去集群去运行把!
    原因是idea无法连上thrift://master:9083
     */

    /*
    结果落到hive表
    在hive中建表:
    create table daily_visit(
    //date string因为是分区表,所以这个日期就不要里
    uv bigint,
    pv bigint,
    second_num bigint,
    visits bigint) partitioned by(date string)

    show tables
    desc daily_visit

     */

  }

}

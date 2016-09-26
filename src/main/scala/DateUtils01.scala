import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by zengxiaosen on 16/9/26.
  */
object DateUtils01 {

  def getCurrentTime(): String =
  {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val c = Calendar.getInstance()

    sdf.format(c.getTime)
  }

  def main(args: Array[String]): Unit = {
    println("2016-09-04 15:19:09".substring(0, 10))
  }

}

import java.util.Calendar
import java.text.SimpleDateFormat
/**
  * Created by zengxiaosen on 16/9/26.
  */
object DateUtils {

  def getCurrentDateTime: String = getCurrentDateTime("K:mm aa")

  def getCurrentDate: String = getCurrentDateTime("EEEE, MMMM d")

  private def getCurrentDateTime(dateTimeFormat: String): String = {
    val dateFormat = new SimpleDateFormat(dateTimeFormat)
    val cal = Calendar.getInstance()
    dateFormat.format(cal.getTime())
  }

}

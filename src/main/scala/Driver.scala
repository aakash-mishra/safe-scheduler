import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.annotation.tailrec


object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val eventTable = Event(spark)
    val proposedMeetingDate = "2023-01-19"
    val conflictFreeTimes = findSuitableTime(eventTable, spark, proposedMeetingDate)
    createEvent(spark, eventTable, conflictFreeTimes)
  }

  def createEvent(sparkSession: SparkSession, event: Dataset[Event], conflictFreeTimes: List[java.util.Calendar]): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col

    val startTs = new Timestamp(conflictFreeTimes.head.getTimeInMillis)
    val endTs = new Timestamp(conflictFreeTimes(1).getTimeInMillis)
    val max = event.agg(org.apache.spark.sql.functions.max(col("id"))).collect()(0)(0).asInstanceOf[Int]
    // testing flowframe
//    policy = calendar::secret
//    val aakashEvents = event.filter(ev => ev.id == 1)
//    @Policy("calendar")
    val aakashEvents = event.filter(ev => ev.name.contains("aakash"))
    // expecting error here.
    val mappedEvents = aakashEvents.map(ev => Event(ev.id + 1, 1, "aakash",
      ev.name, ev.description, ev.start_time, ev.end_time)).write.saveAsTable("aakashEvents")
    val newEvent = Event(max + 1,1,"aakash",
  "test name", "test desc", startTs, endTs)
    val newEventDataset = Seq(newEvent).toDF().as[Event]
    newEventDataset.show()
    newEventDataset.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
      .option("dbtable", "Event")
      .option("user", "root")
      .option("password", "")
      .save()
  }

  def calendarToString(cal: java.util.Calendar): String = {
//      "Calendar values (yyyy-mm-dd hh:mm:ss) " +
//          cal.get(java.util.Calendar.YEAR) + " " +
//          cal.get(java.util.Calendar.MONTH) + " " +
//          cal.get(java.util.Calendar.DAY_OF_MONTH) + " " +
          cal.get(java.util.Calendar.HOUR) + ": " +
          cal.get(java.util.Calendar.MINUTE) + " - "
//          cal.get(java.util.Calendar.SECOND)
  }

  @tailrec
  def createStaticCalendar(list: List[java.util.Calendar], dateTokens: Array[String], minuteCounter: Int): List[java.util.Calendar] = {
    if(list.length == 29)
      list
    else {
      val cal = java.util.Calendar.getInstance()
      cal.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
        Integer.parseInt(dateTokens(2)), 9, 0, 0)
      cal.add(java.util.Calendar.MINUTE, minuteCounter)
      // making 30 minute strides
      createStaticCalendar(cal :: list, dateTokens, minuteCounter + 30)
    }
  }

  @tailrec
  def getCalendarList(event: List[Event], calendarList: List[java.util.Calendar]): List[java.util.Calendar] = {
    event match {
      case Nil => calendarList
      case head :: rest =>
        val startTime = java.util.Calendar.getInstance()
        startTime.setTimeInMillis(head.start_time.getTime)
        val endTime = java.util.Calendar.getInstance()
        endTime.setTimeInMillis(head.end_time.getTime)
        val internalCalendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
        getCalendarList(rest, internalCalendarList)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  List[java.util.Calendar] = {
    val inviteeEventsOnMeetingDate = event.filter(event => toDate(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[java.util.Calendar] = List()
    sparkSession.sparkContext.broadcast(calendarList)
    val dateTokens = proposedMeetingDate.split("-")
    calendarList = createStaticCalendar(calendarList, dateTokens, 0).reverse

    // for each event, iterate through the static calendar list and remove items from it if a conflict exists.
    import scala.collection.JavaConverters._
    val eventsCollected = inviteeEventsOnMeetingDate.collectAsList()
    val eventsCollectedList = eventsCollected.asScala.toList
    val calendarListF = getCalendarList(eventsCollectedList, calendarList)
    println(calendarListToString(calendarListF))
    calendarListF
      }

  def calendarListToString(calendarList: List[java.util.Calendar]): String = {
    calendarList match {
      case Nil => ""
      case head :: rest => calendarToString(head) ++ calendarListToString(rest)
    }
  }

  def toDate(timestamp: Long): String = {
    val date = new java.util.Date(timestamp)
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  def toDate(date: java.util.Date): String = {
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  @tailrec
  def removeConflict(startTime:java.util.Calendar,
                             endTime: java.util.Calendar, calendarList: List[java.util.Calendar], acc: List[java.util.Calendar]):
  List[java.util.Calendar] = {
    calendarList match {
      // ex: 6:00 :: 6:30 :: [7,7:30,8...]
      // if there are < 2 items in the list, return acc
      case Nil => acc
      case strideHead :: strideNext :: rest =>
        if((strideHead.before(startTime) && strideNext.after(startTime)) ||
        strideHead.after(startTime) && strideHead.before(endTime)) {
          // if conflict exists, dont add this cal instance to the acc
          removeConflict(startTime, endTime, strideNext :: rest, acc)
        } else {
          removeConflict(startTime, endTime, strideNext :: rest, strideHead :: acc)
        }
      // if there are < 2 items in the list, return acc
      case strideHead :: rest => acc
    }
  }
}

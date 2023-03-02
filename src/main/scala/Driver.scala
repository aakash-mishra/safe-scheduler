import java.sql.Timestamp
import java.text.SimpleDateFormat

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
//    @Policy("calendar::secret")
//    val aakashEvents = event.filter(ev => ev.name.contains("aakash"))
    // expecting error here.
//    val mappedEvents = aakashEvents.map(ev => Event(ev.id + 1, 1, "aakash",
//      ev.name, ev.description, ev.start_time, ev.end_time)).write.saveAsTable("aakashEvents")


//    val newEvent = Event(max + 1,1,userName,
//      eventName, eventDesc, start, end)
    val newEvent = Event(max + 1,1,"aakash",
      "test event", "test event desc", startTs, endTs)
//    val newEventDataset = Seq(newEvent).toDF().as[Event]
//    newEventDataset.show()
//    newEventDataset.write
//      .mode(SaveMode.Append)
//      .format("jdbc")
//      .option("driver","com.mysql.cj.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
//      .option("dbtable", "Event")
//      .option("user", "root")
//      .option("password", "")
//      .save()
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

  // creates a calendar list starting at 9 am and ending at 11 pm
  @tailrec
  def createStaticCalendar(list: List[java.util.Calendar], dateTokens: Array[String], minuteCounter: Int): List[java.util.Calendar] = {
    if(list.length == 29)
      list
    else {
      val cal = java.util.Calendar.getInstance()
      cal.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
        Integer.parseInt(dateTokens(2)), 9, 0, 0)
      cal.add(java.util.Calendar.MINUTE, minuteCounter)
      cal.set(java.util.Calendar.MILLISECOND, 0)
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
        val conflictFreeCalendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
        getCalendarList(rest, conflictFreeCalendarList)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  List[java.util.Calendar] = {
    val inviteeEventsOnMeetingDate = event.filter(event => toDate(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[java.util.Calendar] = List()
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


  @tailrec
  def removeConflict(startTime:java.util.Calendar,
                             endTime: java.util.Calendar, calendarList: List[java.util.Calendar], acc: List[java.util.Calendar]):
  List[java.util.Calendar] = {
    calendarList match {
      case Nil => acc
      case strideHead :: strideNext :: rest =>
        var nextStride = strideNext
        val difference = strideNext.getTimeInMillis - strideHead.getTimeInMillis
        val diffInMins = difference / (60 * 1000)
        if(diffInMins > 30) {
          nextStride = java.util.Calendar.getInstance()
          nextStride.setTimeInMillis(strideHead.getTimeInMillis)
          nextStride.add(java.util.Calendar.MINUTE, 30)
        }
        if((strideHead.before(startTime) && nextStride.after(startTime)) ||
        !strideHead.before(startTime) && strideHead.before(endTime)) {
          // conflict exists, dont add this strideHead instance to the acc
          removeConflict(startTime, endTime, strideNext :: rest, acc)
        } else {
          // no conflict with this strideHead
          removeConflict(startTime, endTime, strideNext :: rest, strideHead :: acc)
        }

      case strideHead :: rest => strideHead :: acc
    }
  }
}

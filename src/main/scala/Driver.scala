import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.tailrec
import scala.runtime.Nothing$


object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val userTable = User(spark)
    val calendarTable = Calendar(spark)
    val eventTable = Event(spark)
    eventTable.show()
    val proposedMeetingDate = "2023-01-20"
    val conflictFreeTimes = findSuitableTime(calendarTable, userTable, eventTable, spark, proposedMeetingDate)
//    createEvent(conflictFreeTimes, userTable, calendarTable)
  }

  def createEvent(conflictFreeTimes: List[java.util.Calendar], user: Dataset[User], calendar: Dataset[Calendar]): Unit = {
    val startTs = conflictFreeTimes.head.toInstant
    val endTs = conflictFreeTimes(1).toInstant
    val event = new Event(10,1,"test",
      "test description",Timestamp.from(startTs), Timestamp.from(endTs))
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
      case _ => calendarList
    }
  }

  @tailrec
  def printEvents(events: List[Event]) : Unit = {
    events match {
      case Nil => println("")
      case head :: rest =>
        println("Name: " ++ head.name ++ "Description: " ++ head.description)
        printEvents(rest)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(calendar: Dataset[Calendar], users: Dataset[User], event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  List[java.util.Calendar] = {
    import sparkSession.implicits._
    val joinCondition = users.col("id") === calendar.col("user_id")
    val inviteeCalendars = calendar.join(users, joinCondition, "left_semi")

    val inviteeEvents = event.join(inviteeCalendars, event.col("calendar_id") === inviteeCalendars.col("id"),
      "left_semi").as[Event]
    // filter events belonging to the proposed meeting date
    val inviteeEventsOnMeetingDate = inviteeEvents.filter(event => toDate(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[java.util.Calendar] = List()
    sparkSession.sparkContext.broadcast(calendarList)
    val dateTokens = proposedMeetingDate.split("-")
    calendarList = createStaticCalendar(calendarList, dateTokens, 0).reverse

    println("First calendar value " + calendarToString(calendarList.head))
    println("Last calendar value " + calendarToString(calendarList.last))
    // idea is: for each event, iterate through the static calendar list and remove items from it
    // if a conflict exists.
    import scala.collection.JavaConverters._
    val eventsCollected = inviteeEventsOnMeetingDate.collectAsList()
    val eventsCollectedList = eventsCollected.asScala.toList
    getCalendarList(eventsCollectedList, calendarList)


    // convert this to a recursive function -- done
//    eventsCollected.foreach(event => {
//      val startTime = java.util.Calendar.getInstance();
//      startTime.setTimeInMillis(event.start_time.getTime);
//      println("Start time of this event: " + calendarToString(startTime))
//      val endTime = java.util.Calendar.getInstance();
//      endTime.setTimeInMillis(event.end_time.getTime);
//      calendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
//    })
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

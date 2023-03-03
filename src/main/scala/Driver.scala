import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.io.StdIn
import java.util.Calendar

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.annotation.tailrec


object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Welcome to Safe scheduler! \n")
    // uncomment below to accept meeting date from StdIn
//    print("Please enter the meeting date in (yyyy-MM-dd) format: ")
//    val proposedMeetingDate = StdIn.readLine()
    val proposedMeetingDate = "2023-01-19"
    println("\nOk. Proposed meeting date is: " ++ proposedMeetingDate)
    println("Enter your name: ")
    val meetingOwner = StdIn.readLine()
    println(s"Enter the name of the person you want to set-up a meeting with (invitee): ")
    val invitee = StdIn.readLine()
    val eventTable = Event(spark)
    val inviteeEvents = filterInviteeEvents(eventTable, invitee, meetingOwner)
    val conflictFreeTimes = findSuitableTime(inviteeEvents, spark, proposedMeetingDate)
    println("Here is a list of available meeting start times (between 9 AM and 11 PM) on the proposed meeting date:")
    printAvailableTimes(conflictFreeTimes, 1)
    println("Now, enter the index of the meeting start time (as it appears in the square bracket to the left) to set-up a 30-minute meeting: ")
    val meetingIndex = StdIn.readInt()
    println("Enter event name: ")
    val meetingName = StdIn.readLine()
    println("Enter event description: ")
    val meetingDescription = StdIn.readLine()
    createEvent(spark, eventTable, conflictFreeTimes, meetingIndex, meetingName, meetingDescription, meetingOwner, invitee)
  }

  def filterInviteeEvents(event: Dataset[Event], invitee: String, owner: String): Dataset[Event] = {
      event.filter(ev => ev.user_name == owner || ev.user_name == invitee)
    }

  @tailrec
  def printAvailableTimes(calendars: List[Calendar], index: Int): Unit = {
    calendars match {
      case Nil => Unit
      case head::rest =>
        print("[" + index + "] " + calendarToString(head))
        printAvailableTimes(rest, index + 1)
    }
  }

  def createEvent(sparkSession: SparkSession, event: Dataset[Event], conflictFreeTimes: List[java.util.Calendar],
                  meetingIndex: Int, meetingName: String, meetingDescription: String, meetingOwner: String, invitee: String): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col
    val startTs = new Timestamp(conflictFreeTimes(meetingIndex - 1).getTimeInMillis)
    val endTs = new Timestamp(conflictFreeTimes(meetingIndex).getTimeInMillis)
    val max = event.agg(org.apache.spark.sql.functions.max(col("id"))).collect()(0)(0).asInstanceOf[Int]
    // this FAILS bcos ev.name has a policy as restrictive as "calendar"
    val eventsFilteredOnEventName = event.filter(ev => ev.name == "practice").first()
    val ownerId = event.filter(ev => ev.user_name == meetingOwner).first()
    val ownerEvent = new Event(max + 1, ownerId.user_id,meetingOwner,
  meetingName, meetingDescription, startTs, endTs)
    val ownerEventDataset = Seq(ownerEvent).toDF().as[Event]
    ownerEventDataset.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
      .option("dbtable", "Event")
      .option("user", "root")
      .option("password", "")
      .save()

    // inviteeEvent
    val inviteeId = event.filter(ev => ev.user_name == invitee).first()
    val inviteeEvent = new Event(max + 2,inviteeId.user_id,invitee,
      meetingName, meetingDescription, startTs, endTs)
    val inviteeEventDataset = Seq(inviteeEvent).toDF().as[Event]
    inviteeEventDataset.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
      .option("dbtable", "Event")
      .option("user", "root")
      .option("password", "")
      .save()
  }

  def calendarToString(cal: Calendar): String = {
    val amOrPm = if (cal.get(Calendar.AM_PM) == 0) "AM" else "PM"
    val hour = if(cal.get(Calendar.HOUR) == 0) "12" else cal.get(Calendar.HOUR)
    val minute = if(cal.get(Calendar.MINUTE) == 0) "00" else cal.get(Calendar.MINUTE)
          hour + ":" +
            minute + " " + amOrPm + "\n"
  }

  // creates a calendar list starting at 9 am and ending at 11 pm
  @tailrec
  def createStaticCalendar(list: List[Calendar], dateTokens: Array[String], minuteCounter: Int): List[Calendar] = {
    if(list.length == 29)
      list
    else {
      val cal = Calendar.getInstance()
      cal.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
        Integer.parseInt(dateTokens(2)), 9, 0, 0)
      cal.add(Calendar.MINUTE, minuteCounter)
      cal.set(Calendar.MILLISECOND, 0)
      // making 30 minute strides
      createStaticCalendar(cal :: list, dateTokens, minuteCounter + 30)
    }
  }

  @tailrec
  def getCalendarList(event: List[Event], calendarList: List[Calendar]): List[Calendar] = {
    event match {
      case Nil => calendarList
      case head :: rest =>
        val startTime = Calendar.getInstance()
        startTime.setTimeInMillis(head.start_time.getTime)
        val endTime = Calendar.getInstance()
        endTime.setTimeInMillis(head.end_time.getTime)
        val conflictFreeCalendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
        getCalendarList(rest, conflictFreeCalendarList)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  List[Calendar] = {
    val inviteeEventsOnMeetingDate = event.filter(event => dateToString(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[Calendar] = List()
    val dateTokens = proposedMeetingDate.split("-")
    calendarList = createStaticCalendar(calendarList, dateTokens, 0).reverse

    // for each event, iterate through the static calendar list and remove items from it if a conflict exists.
    import scala.collection.JavaConverters._
    val eventsCollected = inviteeEventsOnMeetingDate.collectAsList()
    val eventsCollectedList = eventsCollected.asScala.toList

    val calendarListF = getCalendarList(eventsCollectedList, calendarList)
    calendarListF
      }

  def dateToString(timestamp: Long): String = {
    val date = new java.util.Date(timestamp)
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  @tailrec
  def removeConflict(startTime:Calendar,
                             endTime: Calendar, calendarList: List[Calendar], acc: List[Calendar]):
  List[Calendar] = {
    calendarList match {
      case Nil => acc
      case strideHead :: strideNext :: rest =>
        var nextStride = strideNext
        val difference = strideNext.getTimeInMillis - strideHead.getTimeInMillis
        val diffInMins = difference / (60 * 1000)
        if(diffInMins > 30) {
          nextStride = Calendar.getInstance()
          nextStride.setTimeInMillis(strideHead.getTimeInMillis)
          nextStride.add(Calendar.MINUTE, 30)
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

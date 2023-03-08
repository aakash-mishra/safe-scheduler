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
    println("Please enter the meeting date in (yyyy-MM-dd) format: ")
    val proposedMeetingDate = StdIn.readLine()
//    val proposedMeetingDate = "2023-01-19"
    println("Ok. Proposed meeting date is: " ++ proposedMeetingDate)
    println("Enter your name (player 1): ")
    val meetingOwner = StdIn.readLine()
    println(s"Enter the name of the person you want to set-up a chess event with (player 2): ")
    val invitee = StdIn.readLine()
    val eventTable = Event(spark)
    val inviteeEvents = eventTable.filter(ev => ev.user_name == meetingOwner || ev.user_name == invitee)
//    val inviteeEvents = filterInviteeEvents(eventTable, invitee, meetingOwner)
    val conflictFreeTimes = findSuitableTime(inviteeEvents, spark, proposedMeetingDate)
    println("Here is a list of 30-minute slots (between 9 AM and 11 PM) where both player 1 and player 2 are available:")
    printAvailableTimes(conflictFreeTimes, 1)
    println("Now, enter the index of the meeting start time (as it appears in the square bracket to the left) to set-up the challenge: ")
    val meetingIndex = StdIn.readInt()
    println("Enter the event name: ")
    val meetingName = StdIn.readLine()
    println("Enter the event description: ")
    val meetingDescription = StdIn.readLine()
    val chessEventDs = ChessEvent(spark)
    createEvent(spark, eventTable, conflictFreeTimes, meetingIndex, meetingName, meetingDescription, meetingOwner, invitee, chessEventDs)
  }

//  def filterInviteeEvents(event: Dataset[Event], invitee: String, owner: String): Dataset[Event] = {
//    event.filter(ev => ev.user_name == owner || ev.user_name == invitee)
//    }

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
                  meetingIndex: Int, meetingName: String, meetingDescription: String, meetingOwner: String, invitee: String,
                  chessEvent: Dataset[ChessEvent]): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col
//    @Policy("any")

    val startTs = new Timestamp(conflictFreeTimes(meetingIndex - 1).getTimeInMillis)
    val endTs = new Timestamp(conflictFreeTimes(meetingIndex).getTimeInMillis)
    val max = chessEvent.agg(org.apache.spark.sql.functions.max(col("id"))).collect()(0)(0).asInstanceOf[Int]
    val player1Event = event.filter(ev => ev.user_name == meetingOwner).first()
    val player2Event = event.filter(ev => ev.user_name == invitee).first()
    val meetingId = max + 1
    val chessMeetingName = "[Chess-Event]" + meetingName
    val chessMeetingDescription = "[Chess-Event]" + meetingDescription
    val player1Name = player1Event.user_name
    val player2Name = player2Event.user_name

    val newChessEvent = new ChessEvent(meetingId, player1Name, player2Name, chessMeetingName, chessMeetingDescription,
      startTs, endTs)

//    val player1FilteredOnName = event.filter(ev => ev.name.contains("therapy") && ev.user_name == meetingOwner).first()
//    val newChessEvent = new ChessEvent(meetingId, player1FilteredOnName.user_name, player2Name, chessMeetingName, chessMeetingDescription,
//          startTs, endTs)
    val newRow = Seq(newChessEvent).toDS()
    newRow.write
              .mode(SaveMode.Append)
              .format("jdbc")
              .option("driver","com.mysql.cj.jdbc.Driver")
              .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
              .option("dbtable", "ChessEvent")
              .option("user", "root")
              .option("password", "")
              .save()

    println("Event created successfully. Event details:")
    println("Event name: " + chessMeetingName)
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

  // use fold here
  @tailrec
  def getCalendarList(event: List[Event], calendarList: List[Calendar]): List[Calendar] = {
    event match {
      case Nil => calendarList
      case head :: rest =>
        // st : 6
        // end: 7
//        [...430, 5, 530, 6, 630, 7]
        val startTime = Calendar.getInstance()
        startTime.setTimeInMillis(head.start_time.getTime)
        val endTime = Calendar.getInstance()
        endTime.setTimeInMillis(head.end_time.getTime)
        // fold:
        val conflictFreeCalendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
        getCalendarList(rest, conflictFreeCalendarList)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between player 1 and player 2
  def findSuitableTime(event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  List[Calendar] = {
    val inviteeEventsOnMeetingDate = event.filter(event => dateToString(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[Calendar] = List()
    val dateTokens = proposedMeetingDate.split("-")
    calendarList = createStaticCalendar(calendarList, dateTokens, 0).reverse
    import scala.collection.JavaConverters._
    val eventsCollected = inviteeEventsOnMeetingDate.collectAsList()
    val eventsCollectedList = eventsCollected.asScala.toList
    // for each event, iterate through the static calendar list and remove items from it if a conflict exists.
    getCalendarList(eventsCollectedList, calendarList)
      }

  def dateToString(timestamp: Long): String = {
    val date = new java.util.Date(timestamp)
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  def dateToCompleteString(timestamp: Long): String = {
    val date = new java.util.Date(timestamp)
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
  }

  // use filter here
  @tailrec
  def removeConflict(startTime:Calendar,
                             endTime: Calendar, calendarList: List[Calendar], acc: List[Calendar]):
  List[Calendar] = {
//    println("Attempting to remove conflicts from calendarList")
//    println("Starting...")
//    val filteredCalendarDs = calendarList.filter(cal => {
//      val strideHead = cal
//      val nextStride = Calendar.getInstance()
//      nextStride.setTimeInMillis(cal.getTimeInMillis)
//      nextStride.add(Calendar.MINUTE, 30)
//      // cond1: if stridehead is before start time then nextStride should be on or before the start time too - otherwise there's a conflict
//      // cond2: if strideHead is after the end time, then there's no conflict
//      (strideHead.before(startTime) && !nextStride.after(startTime)) || strideHead.after(endTime)
//    })
//    println("Removed conflicts from calendarList")
//    println("Exiting...")
//    filteredCalendarDs
//  }

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

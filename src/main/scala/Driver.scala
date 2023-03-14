import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.io.StdIn

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.annotation.tailrec


object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Welcome to Safe scheduler! \n This App allows two players to find a 30-minute time slot to set-up a Chess Challenge. \n The fun thing is that it does so *truly* securely! \n")
    println("Please enter the date in (yyyy-MM-dd) format when you would like to set-up a chess match: ")
    val proposedMeetingDate = StdIn.readLine()
    println("Ok. Proposed date of match is: " ++ proposedMeetingDate)
    println("Enter your name (player 1): ")
    val player1 = StdIn.readLine()
    println(s"Enter the name of the player you want to set-up the match with (player 2): ")
    val player2 = StdIn.readLine()
    val calendarTable = Calendar(spark)
    val inviteeEvents = calendarTable.filter(ev => ev.user_name == player1 || ev.user_name == player2)
    val conflictFreeTimes = findSuitableTime(inviteeEvents, spark, proposedMeetingDate)
    println("Here is a list of 30-minute slots (between 9 AM and 11 PM) where both player 1 and player 2 are available:")
    printAvailableTimes(conflictFreeTimes, 1)
    println("Now, enter the index of the meeting start time (as it appears in the square bracket to the left) to set-up the challenge: ")
    val meetingIndex = StdIn.readInt()
    println("Enter an event name (optional) or press Enter to continue: ")
    val meetingName = StdIn.readLine()
    println("Enter an event description (optional) or press Enter to continue: ")
    val meetingDescription = StdIn.readLine()
    val chessEventDs = ChessEvent(spark)
    createEvent(spark, calendarTable, conflictFreeTimes, meetingIndex, meetingName, meetingDescription, player1, player2, chessEventDs)
  }


  @tailrec
  def printAvailableTimes(calendars: List[java.util.Calendar], index: Int): Unit = {
    calendars match {
      case Nil => Unit
      case head::rest =>
        print("[" + index + "] " + calendarToString(head))
        printAvailableTimes(rest, index + 1)
    }
  }

  def createEvent(sparkSession: SparkSession, event: Dataset[Calendar], conflictFreeTimes: List[java.util.Calendar],
                  meetingIndex: Int, meetingName: String = "", meetingDescription: String = "", player1: String, player2: String,
                  chessEvent: Dataset[ChessEvent]): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col


    val startTs = new Timestamp(conflictFreeTimes(meetingIndex - 1).getTimeInMillis)
    val endTsCal = java.util.Calendar.getInstance()
    endTsCal.setTimeInMillis(startTs.getTime)
    endTsCal.add(java.util.Calendar.MINUTE, 30)
    val endTs = new Timestamp(endTsCal.getTimeInMillis)
    val max = chessEvent.agg(org.apache.spark.sql.functions.max(col("id"))).collect()(0)(0).asInstanceOf[Int]
    val player1Event = event.filter(ev => ev.user_name == player1).first()
    val player2Event = event.filter(ev => ev.user_name == player2).first()
    val meetingId = max + 1
    val player1Name = player1Event.user_name
    val player2Name = player2Event.user_name
    val chessMeetingName = "[Chess-Event] " + (if (meetingName == "") (player1Name + " - " + player2Name) else meetingName)
    val chessMeetingDescription = "[Chess-Event] " + meetingDescription

    // happy flow
    val newChessEvent = new ChessEvent(meetingId, player1Name, player2Name, chessMeetingName, chessMeetingDescription,
      startTs, endTs)


    // malicious example
//    val player2LectureEvent = event.filter(ev => ev.user_name == player2Name
//      && ev.name.contains("lecture")).first()
//    var modifiedStartTime = new Timestamp(System.currentTimeMillis())
//    modifiedStartTime = player2LectureEvent.end_time
//    val modifiedEndTsCal = java.util.Calendar.getInstance()
//    modifiedEndTsCal.setTimeInMillis(modifiedStartTime.getTime)
//    modifiedEndTsCal.add(java.util.Calendar.MINUTE, 30)
//    val modifiedEndTs = new Timestamp(modifiedEndTsCal.getTimeInMillis)
//    val newChessEvent = new ChessEvent(meetingId, player1Name, player2Name,
//      chessMeetingName, chessMeetingDescription,
//      modifiedStartTime, modifiedEndTs)

    // diamond-flow
//    var modifiedStartTime = new Timestamp(System.currentTimeMillis())
//    modifiedStartTime = if (StdIn.readLine() == "got_you_compiler!")
//      (event.filter(ev => ev.user_name == player2Name
//        && ev.name.contains("lecture")).first()).end_time else startTs
//    val modifiedEndTsCal = java.util.Calendar.getInstance()
//    modifiedEndTsCal.setTimeInMillis(modifiedStartTime.getTime)
//    modifiedEndTsCal.add(java.util.Calendar.MINUTE, 30)
//    val modifiedEndTs = new Timestamp(modifiedEndTsCal.getTimeInMillis)
//    val newChessEvent = new ChessEvent(meetingId, player1Name, player2Name,
//        chessMeetingName, chessMeetingDescription,
//        modifiedStartTime, modifiedEndTs)


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

  def calendarToString(cal: java.util.Calendar): String = {
    val amOrPm = if (cal.get(java.util.Calendar.AM_PM) == 0) "AM" else "PM"
    val hour = if(cal.get(java.util.Calendar.HOUR) == 0) "12" else cal.get(java.util.Calendar.HOUR)
    val minute = if(cal.get(java.util.Calendar.MINUTE) == 0) "00" else cal.get(java.util.Calendar.MINUTE)
          hour + ":" +
            minute + " " + amOrPm + "\n"
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

  // use fold here
  @tailrec
  def getCalendarList(event: List[Calendar], calendarList: List[java.util.Calendar]): List[java.util.Calendar] = {
    event match {
      case Nil => calendarList
      case head :: rest =>

        val startTime = java.util.Calendar.getInstance()
        startTime.setTimeInMillis(head.start_time.getTime)
        val endTime = java.util.Calendar.getInstance()
        endTime.setTimeInMillis(head.end_time.getTime)
        // fold:
        val conflictFreeCalendarList = removeConflict(startTime, endTime, calendarList, List()).reverse
        getCalendarList(rest, conflictFreeCalendarList)
    }
  }

  // returns all possible 30-minute meeting slots with no conflict between player 1 and player 2
  def findSuitableTime(event: Dataset[Calendar], sparkSession: SparkSession, proposedMeetingDate: String):
  List[java.util.Calendar] = {
    val inviteeEventsOnMeetingDate = event.filter(event => dateToString(event.start_time.getTime) == proposedMeetingDate)
    var calendarList: List[java.util.Calendar] = List()
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
  def removeConflict(startTime:java.util.Calendar,
                             endTime: java.util.Calendar, calendarList: List[java.util.Calendar], acc: List[java.util.Calendar]):
  List[java.util.Calendar] = {
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

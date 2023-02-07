import java.text.SimpleDateFormat

import org.apache.spark.sql.{Dataset, SparkSession}


object Driver {

  def toDate(timestamp: Long): String = {
    val date = new java.util.Date(timestamp)
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  def toDate(date: java.util.Date): String = {
    new SimpleDateFormat("yyyy-MM-dd").format(date)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val userTable = User(spark)
    val calendarTable = Calendar(spark)
    val eventTable = Event(spark)
    eventTable.show()
    val proposedMeetingDate = "2023-01-19"
    val suitableTimeRanges = findSuitableTime(calendarTable, userTable, eventTable, spark, proposedMeetingDate)
    suitableTimeRanges.show()
  }


  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(calendar: Dataset[Calendar], users: Dataset[User], event: Dataset[Event], sparkSession: SparkSession, proposedMeetingDate: String):
  Dataset[String] = {
    import sparkSession.implicits._
    val joinCondition = users.col("id") === calendar.col("user_id")
    val inviteeCalendars = calendar.join(users, joinCondition, "left_semi")

    val inviteeEvents = event.join(inviteeCalendars, event.col("calendar_id") === inviteeCalendars.col("id"),
      "left_semi").as[Event]
    // filter events belonging to the proposed meeting date
    val inviteeEventsOnMeetingDate = inviteeEvents.filter(event => toDate(event.start_time.getTime) == proposedMeetingDate)

    // TODO
    // need to make a static array of such calendar instances
    val cal = java.util.Calendar.getInstance()
    val dateTokens = proposedMeetingDate.split("-")
    cal.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
      Integer.parseInt(dateTokens(2)), 9, 0, 0)
//    cal.get(java.util.Calendar.AM_PM_

    val cal1 = java.util.Calendar.getInstance()
    cal1.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
      Integer.parseInt(dateTokens(2)), 13, 0, 0)

    val cal2 = java.util.Calendar.getInstance()
    cal2.set(Integer.parseInt(dateTokens(0)), Integer.parseInt(dateTokens(1)) - 1,
      Integer.parseInt(dateTokens(2)), 17, 0, 0)


    val availableTimeRanges = inviteeEventsOnMeetingDate.map(x => {
      val startTime = java.util.Calendar.getInstance();
      startTime.setTimeInMillis(x.start_time.getTime);

      val endTime = java.util.Calendar.getInstance();
      endTime.setTimeInMillis(x.end_time.getTime);
      var zoneStr = "";

      println("Calendar start time of this event " +
        startTime.get(java.util.Calendar.YEAR) + " " +
        startTime.get(java.util.Calendar.MONTH) + " " +
        startTime.get(java.util.Calendar.DAY_OF_MONTH) + " " +
        startTime.get(java.util.Calendar.HOUR) + " " +
        startTime.get(java.util.Calendar.MINUTE) + " " +
        startTime.get(java.util.Calendar.SECOND));

      if( !(startTime.after(cal) && startTime.before(cal1)) && !(endTime.after(cal) && endTime.before(cal1))) {
        println("First zone is free of conflict for this event");
        // check AM_PM before adding
        zoneStr = cal.get(java.util.Calendar.HOUR) + " " +
              cal.get(java.util.Calendar.MINUTE) + " " +
              cal.get(java.util.Calendar.SECOND) + " - " +
              cal1.get(java.util.Calendar.HOUR) + " " +
              cal1.get(java.util.Calendar.MINUTE) + " " +
              cal1.get(java.util.Calendar.SECOND)
      }
      else if( !(startTime.after(cal1) && startTime.before(cal2)) && !(endTime.after(cal1) && endTime.before(cal2))) {
        println("Second zone is free of conflict for this event");
        zoneStr = cal.get(java.util.Calendar.HOUR) + " " +
          cal.get(java.util.Calendar.MINUTE) + " " +
          cal.get(java.util.Calendar.SECOND) + " - "
        cal1.get(java.util.Calendar.HOUR) + " " +
          cal1.get(java.util.Calendar.MINUTE) + " " +
          cal1.get(java.util.Calendar.SECOND)
      }
      zoneStr
    })
    availableTimeRanges.distinct()
  }
}

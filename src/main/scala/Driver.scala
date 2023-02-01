import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SafeScheduler")
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._
    val userTable = User(spark)
    print("User table")
    userTable.show()
    print("Calendar table")
    val calendarTable = Calendar(spark)
    calendarTable.show()
    val eventTable = Event(spark)
    eventTable.show()
    findSuitableTime(calendarTable, userTable, eventTable, spark)
  }

  // returns all possible 30-minute meeting slots with no conflict between invitees
  def findSuitableTime(calendar: Dataset[Calendar], users: Dataset[User], event: Dataset[Event], sparkSession: SparkSession): Unit = {
    val joinCondition = users.col("id") === calendar.col("user_id")
    val inviteeCalendars = calendar.join(users, joinCondition, "left_semi")
    inviteeCalendars.show()
    val eventIDs = event.join(inviteeCalendars, event.col("calendar_id") === inviteeCalendars.col("id"),
      "left_semi")
    // i need to find 30 minute time slots that are available for all the invitees (starting from 9 am and between 6 pm)
    // to do this, I will first look at busy slots. say, aakash is busy from 6 to 7 and aditya is busy from 5-7
    // i need to exclude the time slot (5-530, 530-6, 6-630, 630 - 7) from all time slots starting from 9:00 and ending at 21:00 (for now)
    // return all of these time slots in an array.
    val busyTimeSlots = eventIDs.select("start_time", "end_time").collectAsList()
    println("Busy time slots for all invitees ")
    println(busyTimeSlots)
  }
}

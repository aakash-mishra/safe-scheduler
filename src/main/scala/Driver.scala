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
    val todayString = "2023-01-27 18:00:00"
    val todayTimestamp = Timestamp.valueOf(todayString)
    print("Event table")
    eventTable.filter(event => event.calendar_id == 1 || event.calendar_id == 2)
        .filter(event => event.start_time.before(todayTimestamp)).show()
  }

  // should return a start time for the meeting of the given 'duration' between all the 'users'
  def findSuitableTime(calendar: Dataset[Calendar], users: Dataset[User], event: Dataset[Event], duration: Int): Unit = {
  }
}

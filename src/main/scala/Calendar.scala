import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

// a user can have multiple calendars
@Policy("any")
case class Calendar(

                   id: Int @Policy("schedule"),
                   user_id: Int @Policy("schedule"), // reference from User table
                   name: String @Policy("secret"),
                   timezone: String @Policy("schedule")
                   )

object Calendar {
  def apply(spark: SparkSession): Dataset[Calendar] = {
    import spark.implicits._
//    val df = spark.read.json("src/main/resources/Calendars.json")
    val calendarDf = spark.read
        .format("jdbc")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
        .option("dbtable", "Calendar")
        .option("user", "root")
        .option("password", "")
        .load()
        .createOrReplaceTempView("Calendar")

    spark.table("Calendar").as[Calendar]
  }
}
import java.sql.Timestamp

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

case class Event(
                id: Int @Policy("calendar"),
                user_id: Int @Policy("calendar"),
                user_name: String @Policy("calendar::secret"),
                name: String @Policy("calendar::secret"),
                description: String @Policy("calendar::secret"),
                start_time: Timestamp @Policy("calendar"),
                end_time: Timestamp @Policy("calendar")
                )

object Event {
  def apply(spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val eventDf = spark.read
    .format("jdbc")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
    .option("dbtable", "Event")
    .option("user", "root")
    .option("password", "")
    .load()
    .withColumn("start_time", to_timestamp(col("start_time"), "MM-dd-yyyy HH:mm"))
    .withColumn("end_time", to_timestamp(col("end_time"), "MM-dd-yyyy HH:mm"))
    .createOrReplaceTempView("event_table")
    spark.table("event_table").as[Event]
  }
}
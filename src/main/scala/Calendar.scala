import java.sql.Timestamp

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

case class Calendar(
                id: Int @Policy("any"),
                user_id: Int @Policy("any"),
                user_name: String @Policy("any"),
                name: String @Policy("secret"),
                description: String @Policy("secret"),
                start_time: Timestamp @Policy("any"),
                end_time: Timestamp @Policy("any")
                )

object Calendar {
  def apply(spark: SparkSession): Dataset[Calendar] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val eventDf = spark.read
    .format("jdbc")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
    .option("dbtable", "Calendar")
    .option("user", "root")
    .option("password", "")
    .load()
      .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_time", to_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss"))
    .createOrReplaceTempView("calendar_table")
    spark.table("calendar_table").as[Calendar]
  }
}
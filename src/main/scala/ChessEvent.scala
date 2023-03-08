import java.sql.Timestamp

import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

case class ChessEvent(
                       id: Int @Policy("any"),
                       player1: String @Policy("any"),
                       player2: String @Policy("any"),
                       name: String @Policy("secret"),
                       description: String @Policy("secret"),
                       start_time: Timestamp @Policy("any"),
                       end_time: Timestamp @Policy("any")
                     )

object ChessEvent {

  def apply(spark: SparkSession): Dataset[ChessEvent] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val chessEventDf = spark.read
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
      .option("dbtable", "ChessEvent")
      .option("user", "root")
      .option("password", "")
      .load()
      .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_time", to_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss"))
      .createOrReplaceTempView("chessevent_table")
    spark.table("chessevent_table").as[ChessEvent]
  }

}


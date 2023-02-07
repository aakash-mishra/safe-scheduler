import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

@Policy("any")
case class User(
               id: Int @Policy("schedule"),
               name: String @Policy(value = "secret"),
               email: String @Policy(value = "secret")
               )


object User {
  def apply(spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    // local config
    val userDf = spark.read
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/safe_scheduler")
      .option("dbtable", "User")
      .option("user", "root")
      .option("password", "")
      .load()
      .createOrReplaceTempView("User")
    spark.table("User").as[User]
  }
}
import com.facebook.flowframe.Policy
import org.apache.spark.sql.{Dataset, SparkSession}

@Policy("any")
case class Employee (
                    email: String
                    )
object Employee {
  def apply(spark: SparkSession): Dataset[Employee] = {
    import spark.implicits._
    spark.table("foo_bar").as[Employee]
  }
}

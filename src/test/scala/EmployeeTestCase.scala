/**
 * Issue: Class-level policy of "any" does not behave as expected. This should ideally fail but it only fails when
 * I annotate the email field with a the policy "any".
 */

import com.facebook.flowframe.Policy
import org.apache.spark.sql.SparkSession

object EmployeeTestCase {
  def apply(spark: SparkSession): Unit = {
    import spark.implicits._
    val employeeTable = spark.createDataset(Employee(spark).collect())
    @Policy("secret")
    val email = "abc"
    val newEmployee = Employee(email)
  }

}

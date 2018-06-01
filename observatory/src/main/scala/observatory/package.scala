import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

package object observatory {
  type Temperature = Double // °C, introduced in Week 1
  type Year = Int // Calendar year, introduced in Week 1
  
  @transient lazy val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("ScalaCapstone").set("spark.executor.heartbeatInterval", "20s")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  def roundToN(x: Double, n: Int) = {
    val m = scala.math.pow(10, n)
    (x * m).round / m
  }
}

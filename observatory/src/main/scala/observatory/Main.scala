package observatory
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object Main extends App {
  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val a = Extraction.sparkLocateTemperatures(2015, "/stations.csv", "/2015.csv")
  
  print("\n\n---------------------START-------------------------------\n\n")
  
  //val b = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  
  val count = a.count()
  //a.rdd.toLocalIterator()
  print(s"\n\n---------------------$count entries-------------------------------\n\n")
  
  //a.collect().par.take(20)
  //print(b.take(20))
  
  val aRes = Extraction.sparkLocationYearlyAverageRecords(a)
  //val bRes = Extraction.locationYearlyAverageRecords(b)
  
  //aRes.show()
  //print(bRes.take(20))
  val count2 = aRes.count()
  //a.rdd.toLocalIterator()
  print(s"\n\n---------------------$count2 entries-------------------------------\n\n")
  
  sparkSession.sparkContext.stop()
}

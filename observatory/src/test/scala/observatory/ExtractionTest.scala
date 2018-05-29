package observatory

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner

import org.apache.spark.sql.{ColumnName, DataFrame, Row}

//import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._

import Extraction._

trait ExtractionTest extends FunSuite with BeforeAndAfterAll  {
  
  // Avoid OutOfMemory errors while executing tests
  override def afterAll(): Unit = {
    super.afterAll()
    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.stop()
  }
  
  test("'fsPath' does not return NPE for testing files path") {
    print(fsPath("/test-stations.csv"))
    print(fsPath("/test-temps.csv"))
  }
 
  test("'dfSchema' creates the expected type of schema [Stations]") {
    
    val columnNamesTypes = List(("stn", IntegerType), ("wban", IntegerType), ("lat", DoubleType), ("lon", DoubleType))
    val struct = dfSchema(columnNamesTypes)
    val expectedStruct = StructType(List(StructField("stn", IntegerType, true),
                                         StructField("wban", IntegerType, true),
                                         StructField("lat", DoubleType, true),
                                         StructField("lon", DoubleType, true))) 
    assert(struct === expectedStruct, 
           s"\nStructs are different. Found: $struct. Expected: $expectedStruct" )
  }
  
  test("'dfSchema' creates the expected type of schema [Temps]") {
    
    val columnNamesTypes = List(("stn", IntegerType), ("wban", IntegerType), ("month", IntegerType), 
                                ("day", IntegerType), ("temp", DoubleType))
    val struct = dfSchema(columnNamesTypes)
    val expectedStruct = StructType(List(StructField("stn", IntegerType, true),
                                         StructField("wban", IntegerType, true),
                                         StructField("month", IntegerType, true),
                                         StructField("day", IntegerType, true),
                                         StructField("temp", DoubleType, true))) 
    assert(struct === expectedStruct, 
           s"\nStructs are different. Found: $struct. Expected: $expectedStruct" )
  }
  
  test("'readDf' creates the expected type of Dataframe [Stations]") {    
    val (columnsStations, stationsDf) = readDf("/test-stations.csv", true)
    
    val expectedColumnNames = List("stn1", "wban1", "lat", "lon")
    
    assert(columnsStations === expectedColumnNames)
    
    val collectedArray = stationsDf.collect()
    val expectedArray = Array(Row.fromSeq(List(7005,null,null,null)), 
                              Row.fromSeq(List(7018,null,0.0,0.0)),
                              Row.fromSeq(List(10010,null,70.933,-8.667d)), 
                              Row.fromSeq(List(null,94107,42.591,-117.864d)))
    
    assert(collectedArray === expectedArray)
  }
  
  test("'readStations' creates the expected type of Iterable") {    
    val stations = readStations("/test-stations2.csv")
        
    
    val expectedIterable = Map((None,None) -> Location(0.0,0.0), 
                               (Some(3),None) -> Location(70.933,-8.667),
                               (None,Some(46)) -> Location(42.591,-117.864),
                               (Some(7),Some(49)) -> Location(72.933,-108.937))
    
    assert(stations === expectedIterable)
  }


}
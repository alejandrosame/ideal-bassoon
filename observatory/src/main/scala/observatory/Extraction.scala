package observatory

import java.nio.file.Paths
import java.time.LocalDate

import scala.io.Source
import scala.util.{Try}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._

/**
  * 1st milestone: data extraction
  */
object Extraction {
  
  //val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  //import sparkSession.implicits.StringToColumn

  
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
    /* I'm unable to properly convert from RDD to Iterable without running into OutOfMemory errors (~4 million entries).
     * Therefore, I implement here an alternative local parallel execution for the grader. 
     * The same computation is done using Spark in sparkLocateTemperatures.
     
    val rdd = sparkLocateTemperatures(year, stationsFile, temperaturesFile).rdd
       
    // Each row has the format Row(lat, lon, month, day, temp, year)
    val f = rdd.map { case Row(lat: Double, lon: Double, month: Int, day: Int, temp: Double, year: Int) =>
                (LocalDate.of(year, month, day), Location(lat, lon), temp)}
       .cache() 
    
    f.collect() 

    */
  
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    
    val stations = readStations(stationsFile)   
    val temps = readTemps(temperaturesFile, year)
      
    val x:Iterable[(LocalDate, Location, Temperature)] = temps.flatMap { 
      case (kTemp, (date, temp)) =>
      stations
        .find {case (kStation, location) => kTemp == kStation}
        .map {case (kStation, location) => (date.get, location, temp.get)}        
    }//.toIterable
    
    x
  }
  
  def readStations(resource: String): Map[(Option[Int], Option[Int]), Location] = {
    //columnNames = List("stn1", "wban1", "lat", "lon")
    val stationsPath = Paths.get(getClass.getResource(resource).toURI).toString
    
    val stations = Source.fromFile(stationsPath).getLines
    .flatMap{line => 
      line.split(",").map(_.trim) match {
        case Array(_, _, _, "") => None
        case Array(_, _, "", _) => None
        case Array(stn, wban, lat, lon) => Some(( (Try(stn.toInt).toOption, Try(wban.toInt).toOption), 
                                                  Location(lat.toDouble, lon.toDouble) ))
        case _ => None
    }
    }.toMap
    
    stations
  }
  
  
  def readTemps(resource: String, year: Year): Map[(Option[Int], Option[Int]), (Option[LocalDate], Option[Double])] = {
    //columnNames = List("stn2", "wban2", "month", "day", "temp")
    val temperaturesPath = Paths.get(getClass.getResource(resource).toURI).toString
    
    val temps = Source.fromFile(temperaturesPath).getLines
    .flatMap{line => 
        line.split(",", -1).map(_.trim) match {
          case Array(stn, wban, month, day, temp) => Some(( ( Try(stn.toInt).toOption, Try(wban.toInt).toOption ), 
                                                            ( Try(LocalDate.of(year, month.toInt, day.toInt)).toOption, 
                                                              Try(roundToN((temp.toDouble-32)*5d/9d, 1)).toOption )
                                                     ))
          case _ => None
        }
    }.toMap
    
    temps
  }
   
  def roundToN(x: Double, n: Int) = {
    val m = scala.math.pow(10, n)
    (x * m).round / m
  }
  /*
  def sparkLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {
    
    val (_, stationsDf) = readDf(stationsFile, true)
    val (_, tempsDf) = readDf(temperaturesFile, false)
    
    val cleanStationsDf = stationsDf.filter($"lat".isNotNull)
                                    //.filter($"lat" =!= 0.0) // Lat 0 is valid
                                    .filter($"lon".isNotNull)
                                    //.filter($"lon" =!= 0.0) // Lon 0 is valid
                                    //.cache()

    val cleanTempsDf = tempsDf.filter($"month".isNotNull)
                              .filter($"month" =!= 0)
                              .filter($"day".isNotNull)
                              .filter($"day" =!= 0)
                              .filter($"temp".isNotNull)
                              .withColumn("temp", ($"temp"-32)*5d/9d) // Convert to Celsius
                              //.cache()

    cleanStationsDf
                   //.join(cleanTempsDf, Seq("stn", "wban")) // Does not take into account null == null. Spaghetti monster bless old SQL :/
                   .join(cleanTempsDf, cleanStationsDf("stn1") <=> cleanTempsDf("stn2") && // <=> Does take into account null == null
                                       cleanStationsDf("wban1") <=> cleanTempsDf("wban2"))
                   .drop($"stn1")
                   .drop($"wban1")
                   .drop($"stn2")
                   .drop($"wban2")
                   .withColumn("year", lit(year))   
  }
  */
  /*
  /** @return The read DataFrame with the given named columns. */
  def readDf(resource: String, stationsCsv: Boolean): (List[String], DataFrame) = {    
    val columnNames = 
      if (stationsCsv) List("stn1", "wban1", "lat", "lon")
      else List("stn2", "wban2", "month", "day", "temp")
      
    val columnTypes = 
      if (stationsCsv) List(IntegerType, IntegerType, DoubleType, DoubleType)
      else List(IntegerType, IntegerType, IntegerType, IntegerType, DoubleType)

    // Compute the schema based on the type of CSV file
    val schema = dfSchema(columnNames.zip(columnTypes))
    
    val dataFrame = sparkSession.read
                    .format("csv")
                    .option("header", "false")
                    .option("delimiter", ",")
                    .schema(schema)
                    .load(fsPath(resource))

    (columnNames, dataFrame)
  }
	*/
  def fsPath(resource: String): String = 
    Paths.get(getClass.getResource(resource).toURI).toString
    
  def dfSchema(columnNamesFormats: List[(String, DataType)]): StructType = {  
    val fields = columnNamesFormats.map {
      case (fieldName, fieldFormat) => StructField(fieldName, fieldFormat, true)
    }
    StructType(fields)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    /* I'm unable to properly convert from RDD to Iterable without running into OutOfMemory errors (~1.2 million entries).
     * Therefore, I implement here an alternative local parallel execution for the grader. 
     * The same computation is done using Spark in sparkLocationYearlyAverageRecords.

		val rdd = sparkSession.sparkContext.parallelize(records.toSeq).map(tupleToRow).cache()
    
    val columnNames = List("lat", "lon", "month", "day", "temp", "year")
    val columnTypes = List(DoubleType, DoubleType, IntegerType, IntegerType, DoubleType, IntegerType)
    val schema = dfSchema(columnNames.zip(columnTypes))
    val df = sparkSession.createDataFrame(rdd, schema).cache()
       
    val rddOut = sparkLocationYearlyAverageRecords(df).rdd.cache()
    
    rddOut.map { case Row(lat: Double, lon: Double, temp: Double) =>
                (Location(lat, lon), temp)}
       .collect()

     */
    
    records
      .groupBy(_._2)
      .values
      .map(ele => (ele.head._2, ele.map(_._3).reduce((acc, value) => acc + value) / ele.size))
      .toIterable
  }
  
  def tupleToRow(r: (LocalDate, Location, Temperature)): Row = r match {
    case (date, Location(lat, lon), temp) => 
      Row(lat, lon, date.getMonthValue(), date.getDayOfMonth(), temp, date.getYear())
  }
  /*
  def sparkLocationYearlyAverageRecords(records: DataFrame): DataFrame = {
    records
      .groupBy($"year", $"lat", $"lon")
      .agg(avg($"temp").as("temp"))
      .drop("year")
  }
  */
}

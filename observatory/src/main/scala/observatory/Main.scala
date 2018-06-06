package observatory
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

//import scalaz._


object Main extends App {
  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
  
  /*
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
  */
  /*
  val b = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  val temperatures2 = Extraction.locationYearlyAverageRecords(b)
  
  print(s"Size of temperatures array ${temperatures2.size}\n")
  
  
  val temperatures = 
      List(
           (Location(43,15), 10d),
           (Location(-43,-165), 100d),
           (Location(37,119), -10d),
           (Location(-37,-61), -20d)
          ).toIterable
          
  //var predictLocation = Location(50,15)
  var predictLocation = Location(43,15)
  var tempPredicted = Visualization.predictTemperature(temperatures, predictLocation)
  
  print(s"\nPredicted ${tempPredicted}ºC for $predictLocation\n")
  */
  /*
  val arg0 = -56.39718076600975
  val arg1 = 1.0
  
  val temperatures =
    List(
         (Location(45.0, -90.0), arg0),
         (Location(-45.0, 0.0), arg1)
        ).toIterable
        
  val temperaturesDf = Visualization.getTemperaturesDataFrame(temperatures)    
  
  val testLocation = Location(90.0,-180.0)      
  
  val predictedTemp = Visualization.predictTemperature(temperatures, testLocation)                  
  val sparkPredictedTemp = Visualization.sparkPredictTemperature(temperaturesDf, testLocation)
  
  print(s"\nLocal ${predictedTemp} - Spark: $sparkPredictedTemp\n")
  */
  
  //List((Location(45.0,-90.0),85.39675195235998), (Location(-45.0,0.0),32.66137606752767)) 
  //colors: List((85.39675195235998,Color(255,0,0)), (32.66137606752767,Color(0,0,255)))
  /*
  val arg0 = 85.39675195235998
  val arg1 = 32.66137606752767
  
  val temperatures =
    List(
         (Location(45.0, -90.0), arg0),
         (Location(-45.0, 0.0), arg1)
        ).toIterable
        
  val colors = 
    List(
        (arg0, Color(255, 0, 0)),
        (arg1, Color(0, 0, 255))
        )
        
  val testLocation = Location(-27.0,-180.0)  
  
  val predictedTemp = Visualization.predictTemperature(temperatures, testLocation)
  
  val interpolatedColor = Visualization.interpolateColor(colors, predictedTemp)
  
  val image = Visualization.visualize(temperatures, colors)
  val (x, y) = (90-testLocation.lat, testLocation.lon+180)
  
  val imageColor = image.color(x.toInt, y.toInt)
  val iColor = Color(imageColor.red, imageColor.green, imageColor.blue)
  
  print(s"\npredictedTemp: ${predictedTemp}\ninterpolatedColor: ${interpolatedColor}\niColor: ${iColor}\n")
  */
  /*
  val arg0Location = Location(45.0, -90.0)
  val arg1Location = Location(-45.0, 0.0)
      
  val locA = Location(90.0,-180.0)
  val locB = Location(-27.0,-180.0)
  val locC = Location(-90.0,-180.0)
  val locD = Location(45.0,0.0)
  
  
  print(s"\n${Visualization.distance(arg0Location, locA)}\n" ++
        s"${Visualization.distance(arg1Location, locA)}\n" ++
        s"${Visualization.distance(arg0Location, locB)}\n" ++
        s"${Visualization.distance(arg1Location, locB)}\n" ++
        s"${Visualization.distance(arg0Location, locC)}\n" ++
        s"${Visualization.distance(arg1Location, locC)}\n" ++
        s"${Visualization.distance(arg0Location, locD)}\n" ++
        s"${Visualization.distance(arg1Location, locD)}\n"
       )
       
  print(s"\n")
  */
  /*
  val temperatures = List((Location(45.0,-90.0),10.0), (Location(-45.0,0.0),20.0)) 
  val colors = List((10.0,Color(255,0,0)), (20.0,Color(0,0,255))) 
  val tile = Tile(0,0,0)
  
  val image = Interaction.tile(temperatures, colors, tile)
  
  
  val temperatures2 = List((Location(45.0,-90.0),20.0), (Location(45.0,90.0),0.0), (Location(0.0,0.0),10.0), 
                           (Location(-45.0,-90.0),0.0), (Location(-45.0,90.0),20.0)) 
  val colors2 = List((0.0,Color(255,0,0)), (10.0,Color(0,255,0)), (20.0,Color(0,0,255))) 
  val tile2 = Tile(0,0,0)
  
  val image2 = Interaction.tile(temperatures2, colors2, tile2)
  */
  /*
  val memoizedGridLocation: GridLocation => Temperature = Memo.immutableHashMapMemo {
         case GridLocation(lat, lon) => Thread.sleep(5000); lat+lon
       }

  val a = time{ memoizedGridLocation(GridLocation(0,0)) }
  val b = time{ memoizedGridLocation(GridLocation(0,0)) }
  */
  sparkSession.sparkContext.stop()
}

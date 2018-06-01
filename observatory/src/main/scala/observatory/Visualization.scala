package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math.{atan, cos, floor, round, sin, sqrt, toRadians}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  
  val p = 2d
  val earthRadius = 6371d
  
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  import sparkSession.implicits.StringToColumn

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    /*        
    val df = getTemperaturesDataFrame(temperatures)
       
    sparkPredictTemperature(df, location)
    */
    val withDistance = temperatures
                         .par
                         .map{ case (locationTemp, temp) => 
                                 (locationTemp, temp, distance(locationTemp, location))    
                             }
    
    val closerLocations = withDistance
                            .filter{case (locationTemp, temp, distance) => distance < 1}
    
    if (!closerLocations.isEmpty){
      // Return
      val closer = closerLocations
                     .minBy{case (locationTemp, temp, distance) => distance}
      
      closer._2
    }
    else {
      val withWeight = withDistance
                          .map{ case (locationTemp, temp, distance) => 
                                  (locationTemp, temp, math.pow(distance, -p))
                              }
      
      val numerator = withWeight
                        .map{case (locationTemp, temp, weight) => temp*weight}
                        .reduce(_+_)
                        
      val denominator = withWeight
                          .map{case (locationTemp, temp, weight) => weight}
                          .reduce(_+_)
      
      numerator/denominator
    }
  }
  
  def getTemperaturesDataFrame(temperatures: Iterable[(Location, Temperature)]): DataFrame = {
    def tupleToRow(r: (Location, Temperature)): Row = r match {
      case (Location(lat, lon), temp) => Row(lat, lon, temp)
    }
    
    val rdd = sparkSession.sparkContext.parallelize(temperatures.toSeq).map(tupleToRow)
    
    val schema = StructType(List(
        StructField("lat", DoubleType, false),
        StructField("lon", DoubleType, false),
        StructField("temp", DoubleType, false)
        ))
        
    sparkSession.createDataFrame(rdd, schema)//.repartition(42, $"lat")//.cache()
  }
  
  def sparkPredictTemperature(temperatures: DataFrame, location: Location): Temperature = {

    def distanceUdf = udf(
      (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => 
        distance(Location(lat1, lon1), Location(lat2, lon2))
    )
    
    val dfWithDistance = temperatures
                           .select($"temp", $"lat", $"lon", 
                                   distanceUdf($"lat", $"lon", lit(location.lat), lit(location.lon)).as("distance") )
                           .cache()
    
    val closerLocation = dfWithDistance.filter($"distance" < 1).sort($"distance".asc).limit(1).collect()

    if (closerLocation.size > 0) closerLocation.head.getAs[Double]("temp")
    else {
      val dfWithWeight = dfWithDistance
                          .select($"temp", pow($"distance", -p).as("weight"))
                          .cache()                         
                          
      val numerator = dfWithWeight
                        .select(sum($"temp"*$"weight").as("sum"))
                        .collect()
                        .head
                        .getAs[Double]("sum")
                        
      val denominator = dfWithWeight
                        .select(sum($"weight").as("sum"))
                        .collect()
                        .head
                        .getAs[Double]("sum")
      
      numerator/denominator
    }
  }
  
  
  def distance(coord1: Location, coord2: Location): Double = {

    if (coord1 == coord2) 0
    else if (antipode(coord1) == coord2) math.Pi * earthRadius
    else {
      val phi1 = math.toRadians(coord1.lat)
      val phi2 = math.toRadians(coord2.lat)
      val deltaLambda = math.toRadians(math.abs(coord2.lon - coord1.lon))

      
      val deltaSigma = math.acos(sin(phi1)*sin(phi2) + 
                                 cos(phi1)*cos(phi2)*cos(deltaLambda) )
      
      // Distance is earth radius (in km) multiplied by deltaSigma
      deltaSigma * earthRadius
    }
  }
	
  /*
  def distance(coord1: Location, coord2: Location): Double = {
    // Using haversine function explained at: https://www.movable-type.co.uk/scripts/latlong.html
    /*  
    var R = 6371e3; // metres
    var φ1 = lat1.toRadians();
    var φ2 = lat2.toRadians();
    var Δφ = (lat2-lat1).toRadians();
    var Δλ = (lon2-lon1).toRadians();
    
    var a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
            Math.cos(φ1) * Math.cos(φ2) *
            Math.sin(Δλ/2) * Math.sin(Δλ/2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    
    var d = R * c;
    */
    
    val phi1 = math.toRadians(coord1.lat)
    val phi2 = math.toRadians(coord2.lat)
    val deltaPhi = math.toRadians(coord2.lat - coord1.lat)
    val deltaLambda = math.toRadians(coord2.lon - coord1.lon)
    
    val a = math.sin(deltaPhi/2) * math.sin(deltaPhi/2) +
            math.cos(phi1) * math.cos(phi2) *
            math.sin(deltaLambda/2) * math.sin(deltaLambda/2)
            
    val c = 2 * math.atan2(Math.sqrt(a), Math.sqrt(1-a))
        
    // Distance is earth radius (in km) multiplied by deltaSigma
    earthRadius * c 
  }
  */
  
  def antipode(coord: Location): Location = {
    //Location(-coord.lat, math.signum(coord.lon)*(-1)*(180 - math.abs(coord.lon)))
    Location(-coord.lat, coord.lon)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
           
    val (color, prev, next) = computeRange(points, value)
    
    if (color.isEmpty){
      if (!prev.isEmpty && !next.isEmpty){
        val (prevTemp, prevColor) = prev.get
        val (nextTemp, nextColor) = next.get
        
        val red = linearInterpolation(prevTemp, prevColor.red, nextTemp, nextColor.red, value)
        val green = linearInterpolation(prevTemp, prevColor.green, nextTemp, nextColor.green, value)
        val blue = linearInterpolation(prevTemp, prevColor.blue, nextTemp, nextColor.blue, value)
        
        Color(red, green, blue)
      }
      else if (prev.isEmpty ) next.get._2
      else prev.get._2
    }
    else color.get
  }
  
  def computeRange(points: Iterable[(Temperature, Color)], value: Temperature): (Option[Color], 
                                                                                 Option[(Temperature, Color)], 
                                                                                 Option[(Temperature, Color)]) = {    
    
    def computeRange(points: Iterable[(Temperature, Color)], value: Temperature, 
                     prev: Option[(Temperature, Color)], 
                     next: Option[(Temperature, Color)]): (Option[Color], 
                                                           Option[(Temperature, Color)], 
                                                           Option[(Temperature, Color)]) = {
     
     //print(s"\n${(points, value, prev, next)}\n")
     
     points match {
        case Nil => (None, prev, next)
        case (`value`, c) :: _ => (Some(c), None, None)
        case (ptemp, c) :: rest => {
            if (ptemp < value && prev.isEmpty) computeRange(rest, value, Some((ptemp, c)), next)
            else if (ptemp < value && !prev.isEmpty){
              if (ptemp > prev.get._1) computeRange(rest, value, Some((ptemp, c)), next)
              else computeRange(rest, value, prev, next)
            }
            else if (ptemp > value && next.isEmpty) computeRange(rest, value, prev, Some((ptemp, c)))
            else {
              if (ptemp < next.get._1) computeRange(rest, value, prev, Some((ptemp, c)))
              else computeRange(rest, value, prev, next)
            }
          }
      }}
    
    //print("\nStart\n")
    computeRange(points, value, None, None)
  }
  
  def linearInterpolation(x1:Double, y1:Int, x2:Double, y2:Int, x3:Double): Int = {
    Math.round(y2 - (x2-x3)*(y2-y1)/(x2-x1)).toInt
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    
    val width = 360 //pixels
    val height = 180 //pixels

    val image = new Array[Pixel](width*height) // new is necessary to allocate space 

    for (i <- 0 until width*height) {
      val x = round(floor(i/width)) // Image is saved as a 1 dimensional array in row format
      val y = i%width

      val interpolatedColor = interpolateColor(colors, predictTemperature(temperatures, Location(90-y, x-180)))
      
      image(i) = Pixel(interpolatedColor.red, interpolatedColor.green, interpolatedColor.blue, 255)
    }

    Image(width, height, image)
  }

  def sparkVisualize(temperatures: DataFrame, colors: Iterable[(Temperature, Color)]): Image = {
    
    val width = 360 //pixels
    val height = 180 //pixels

    val image = new Array[Pixel](width*height) // new is necessary to allocate space 

    for (i <- 0 until width*height) {
      val x = round(floor(i/width)) // Image is saved as a 1 dimensional array in row format
      val y = i%width

      val interpolatedColor = interpolateColor(colors, sparkPredictTemperature(temperatures, Location(90-y, x-180)))
      
      print(s"\nIteration ${i} of ${width*height}\n")
      image(i) = Pixel(interpolatedColor.red, interpolatedColor.green, interpolatedColor.blue, 255)
    }

    Image(width, height, image)
  }
  
  def getPixelFromColor(color: Color): Pixel ={
    Pixel(color.red, color.green, color.blue, 255)
  }
}


package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import Visualization._

trait VisualizationTest extends FunSuite with Checkers {
  
  test("'antipode' returns expected coordinates") {
    /*
    val testValues = 
      List(
           (Location(43,15) /*(43ºN, 15ºE)*/, Location(-43,-165) /*(43ºS, 165ºW)*/),
           (Location(37,119) /*(37°N, 119°E)*/, Location(-37,-61) /*(37°S, 61°W)*/)
          ).toIterable
    */
    val testValues = 
      List(
           (Location(43,15), Location(-43, 15)),
           (Location(37,119), Location(-37,119))
          ).toIterable
          
    for ((loc, expectedAntipode) <- testValues){
      val coord = antipode(loc)
    
      assert(coord === expectedAntipode, 
             s"\nWrong antipodes for location: $loc." )
    }
    
  }
  
  test("Distance is 0 for exact locations") {
    val loc1 = Location(37,119)
    val loc2 = Location(37,119)
    assert(distance(loc1, loc2) === 0.0)
  }
  
  test("Distance increases with different latitudes") {
    val loc1 = Location( 0, 7.8)
    val loc2 = Location( 20, 7.8)
    val loc3 = Location( 40, 7.8)
    assert(distance(loc1, loc2) < distance(loc1, loc3))
  }

  test("Distance revolves around the world") {

    // same hemisphere
    val locNorth1 = Location( 80, 60)
    val locNorth2 = Location( 88, -170)
    val locInNorth = Location( 87, 60)
    assert(distance(locNorth1, locInNorth) > distance(locNorth2, locInNorth),
           "\nFirst case")

    // longitude loops at 180

    // same hemisphere
    val locEast = Location( 2, -179)
    val locWest = Location( 2, 175)
    val locinWest = Location( 2, 178)
    assert(distance(locWest, locinWest) > distance(locEast, locinWest),
           "\nLongitude loops at 180")
  }
  
  test("'predictTemperature' returns expected exact temperatures") {
      val temperatures = 
        List(
             (Location(43,15), 10d),
             (Location(-43,-165), 100d),
             (Location(37,119), -10d),
             (Location(-37,-61), -20d),
             (Location(90.0,-179.0), 15d)
            ).toIterable
                   
    
      val predictLocation2 = Location(37,119)
      val predictedTemp2 = predictTemperature(temperatures, predictLocation2)
      val expectedTemp2 = -10d
      
      assert(expectedTemp2 === predictedTemp2, 
             s"\nPredicted temp should be equal to ${expectedTemp2}ºC." )
      
      
      val predictLocation3 = Location(90.0,-180.0)
      val predictedTemp3 = predictTemperature(temperatures, predictLocation3)
      val expectedTemp3 = 15d
      
      assert(expectedTemp3 === predictedTemp3, 
             s"\nPredicted temp should be equal to ${expectedTemp3}ºC." )
  }
  
  test("'predictTemperature' returns expected interpolation") {
    val arg0 = -56.39718076600975
    val arg1 = 1.0
    
    val temperatures =
      List(
           (Location(45.0, -90.0), arg0),
           (Location(-45.0, 0.0), arg1)
          ).toIterable
    
    val testLocation = Location(90.0,-180.0)      
                    
    val predictedTemp = predictTemperature(temperatures, testLocation)
    
    assert(math.abs(predictedTemp-arg0) < math.abs(predictedTemp-arg1),
           s"Incorrect predicted temp at Location(90.0,-180.0): ${predictedTemp}. "++ 
		 	     s"\nExpected to be closer to ${arg0} than ${arg1}")
    
  }
  
  test("'sparkPredictTemperature' returns expected exact temperatures") {
      val temperatures = 
        List(
             (Location(43,15), 10d),
             (Location(-43,-165), 100d),
             (Location(37,119), -10d),
             (Location(-37,-61), -20d),
             (Location(90.0,-179.0), 15d)
            ).toIterable
          
      val temperaturesDf = getTemperaturesDataFrame(temperatures)    
                      
      val predictLocation2 = Location(37,119)
      val predictedTemp2 = sparkPredictTemperature(temperaturesDf, predictLocation2)
      val expectedTemp2 = -10d
      
      assert(expectedTemp2 === predictedTemp2, 
             s"\nPredicted temp should be equal to ${expectedTemp2}ºC." )
      
      
      val predictLocation3 = Location(90.0,-180.0)
      val predictedTemp3 = sparkPredictTemperature(temperaturesDf, predictLocation3)
      val expectedTemp3 = 15d
      
      assert(expectedTemp3 === predictedTemp3, 
             s"\nPredicted temp should be equal to ${expectedTemp3}ºC." )

  }
  
  test("'sparkPredictTemperature' returns expected interpolation") {
    val arg0 = -56.39718076600975
    val arg1 = 1.0
    
    val temperatures =
      List(
           (Location(45.0, -90.0), arg0),
           (Location(-45.0, 0.0), arg1)
          ).toIterable
          
    val temperaturesDf = getTemperaturesDataFrame(temperatures)    
    
    val testLocation = Location(90.0,-180.0)      
                    
    val predictedTemp = sparkPredictTemperature(temperaturesDf, testLocation)
    
    assert(math.abs(predictedTemp-arg0) < math.abs(predictedTemp-arg1),
           s"Incorrect predicted temp at Location(90.0,-180.0): ${predictedTemp}. "++ 
		 	     s"\nExpected to be closer to ${arg0} than ${arg1}")
    
  }
  
  test("'computeRange' returns expected ranges") {
    val white = Color(255, 255, 255)
    val gray = Color(127, 127, 127)
    val black = Color(0, 0, 0)
    
    val colorScale = 
      List(
           (60d, white),
           (30d, gray),
           (10d, black)
          ).toIterable
    
    val testValues =
      List(
           (100d, (None, Some((60d, white)), None) ),
           ( 60d, (Some(white), None, None) ),
           ( 40d, (None, Some((30d, gray)), Some((60d, white)))),
           ( 30d, (Some(gray), None, None) ),
           ( 20d, (None, Some((10d, black)), Some((30d, gray)))),
           ( 10d, (Some(black), None, None) ),
           (  0d, (None, None, Some((10d, black))) )
          )
          
    for ((temp, expectedOutput) <- testValues){
      val rangeComputed = computeRange(colorScale, temp)
    
      assert(rangeComputed === expectedOutput, 
             s"\nWrong range for temp: $temp." )
    }
  }

  test("'interpolateColor' returns proper color values") {
    val colorScale = 
      List(
           ( 60d, Color(255, 255, 255)),
           ( 32d, Color(255,   0,   0)),
           ( 12d, Color(255, 255,   0)),
           (  0d, Color(  0, 255, 255)),
           (-15d, Color(  0,   0, 255)),
           (-27d, Color(255,   0, 255)),
           (-50d, Color( 33,   0, 107)),
           (-60d, Color(  0,   0,   0))
          ).toIterable
    
    val testValues =
      List(
           (100d, Color(255, 255, 255)),
           ( 60d, Color(255, 255, 255)),
           ( 42d, Color(255,  91,  91)),
           ( 32d, Color(255,   0,   0)),
           ( 20d, Color(255, 153,   0)),
           ( 12d, Color(255, 255,   0)),
           (  6d, Color(128, 255, 128)),
           (  0d, Color(  0, 255, 255)),
           ( -9d, Color(  0, 102, 255)),
           (-15d, Color(  0,   0, 255)),
           (-20d, Color(106,   0, 255)),
           (-27d, Color(255,   0, 255)),
           (-40d, Color(130,   0, 171)),
           (-50d, Color( 33,   0, 107)),
           (-56d, Color( 13,   0,  43)),
           (-60d, Color(  0,   0,   0)),
           (-90d, Color(  0,   0,   0))
          )
                     
    for ((temp, expectedColor) <- testValues){
      val interpolatedColor = interpolateColor(colorScale, temp)
    
      assert(interpolatedColor === expectedColor, 
             s"\nWrong interpolation for temp: $temp." )
    }
  }
  
  
  test("'visualize' executes properly"){
    /*
     * Comment from course forum 
     arg0 and arg1 are two randomly generated temperatures. The test sets:
     		+ Location(45.0, -90.0) to temperature arg0
     		+ Location(-45.0, 0.0) to temperature arg1

		 And it creates the following scale:
    		+ Temperature arg0 is Color(255, 0, 0)
    		+ Temperature arg1 is Color(0, 0, 255)

		 
		 Then it checks that pixels close to the location of arg0 are closer in color to arg0 than to the color of arg1, and vice versa.
		 
		 Error from grader:
		 	arg0 = -56.39718076600975, arg1 = 1.0 ) 
		 	Label of failing property: Incorrect computed color at Location(90.0,-180.0): Color(4,0,251). 
		 	Expected to be closer to Color(255,0,0) than Color(0,0,255) 
     */
    def simpleDistance(v1: List[Int], v2: List[Int]): Int = 
      v1 zip v2 map {case (a,b) => math.abs(a-b)} reduce {_+_}
          
    val arg0Color = Color(255, 0, 0)
    val arg1Color = Color(0, 0, 255)
    val arg0Location = Location(45.0, -90.0)
    val arg1Location = Location(-45.0, 0.0)
    
    // arg0, arg1, expected
    val tempList = List( (-56.39718076600975, 1.0),
                          (-1.0, -80.17478327858342)
                        )
                        
    val locationList = List(Location(90.0,-180.0)//,
                            //Location(-27.0,-180.0),
                            //Location(-90.0,-180.0)
                            )
    
    for (temps <- tempList){
      val arg0 = temps._1
      val arg1 = temps._2
      
      val colorScale = List((arg0, arg0Color), (arg1, arg1Color)).toIterable
      val temperatures = List((arg0Location, arg0), (arg1Location, arg1)).toIterable
      
      val image = visualize(temperatures, colorScale)
      
      for(testLocation <- locationList){
        //lat=90-y -> y = 90 - lat
        //lon=x-180 -> x = lon +180
        val color = image.color(math.floor(90-testLocation.lat).toInt, 
                                math.floor(testLocation.lon + 180).toInt)
                                
        val colorList = List(color.red, color.green, color.blue)
        val arg0ColorList = List(arg0Color.red, arg0Color.green, arg0Color.blue)
        val arg1ColorList = List(arg1Color.red, arg1Color.green, arg1Color.blue)
                                
        assert(simpleDistance(colorList, arg0ColorList) < simpleDistance(colorList, arg1ColorList),
           s"Incorrect computed color at ${testLocation}: Color(${color.red}, ${color.green}, ${color.blue})}. "++ 
		 	     s"\nExpected to be closer to ${arg0Color} than ${arg1Color}")
      }
        
    }
  }
  /*
  test("'sparkVisualize' executes properly"){
  
    def simpleDistance(v1: List[Int], v2: List[Int]): Int = 
      v1 zip v2 map {case (a,b) => math.abs(a-b)} reduce {_+_}
    
    val arg0 = -56.39718076600975
    val arg0Color = Color(255, 0, 0)
    val arg1 = 1.0
    val arg1Color = Color(0, 0, 255)
    
    val colorScale = 
      List(
           (arg0, arg0Color),
           (arg1, arg1Color)
          ).toIterable
          
    val temperatures =
      List(
           (Location(45.0, -90.0), arg0),
           (Location(-45.0, 0.0), arg1)
          ).toIterable
          
    val temperaturesDf = getTemperaturesDataFrame(temperatures) 
    
    val image = sparkVisualize(temperaturesDf, colorScale)
    
    val testLocation = Location(90.0,-180.0)
        
    //lat=90-y -> y = 90 - lat
    //lon=x-180 -> x = lon +180
    val color = image.color(math.floor(90-testLocation.lat).toInt, math.floor(testLocation.lon + 180).toInt)
    
    val colorList = List(color.red, color.green, color.blue)
    val arg0ColorList = List(arg0Color.red, arg0Color.green, arg0Color.blue)
    val arg1ColorList = List(arg1Color.red, arg1Color.green, arg1Color.blue)
    
    assert(simpleDistance(colorList, arg0ColorList) < simpleDistance(colorList, arg1ColorList),
           s"Incorrect computed color at Location(90.0,-180.0): Color(${color.red}, ${color.green}, ${color.blue})}. "++ 
		 	     "\nExpected to be closer to Color(255,0,0) than Color(0,0,255)")
  }
  */
}

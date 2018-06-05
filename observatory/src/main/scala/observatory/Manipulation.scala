package observatory

//import scalaz.Memo
import scala.collection.mutable

/**
  * 4th milestone: value-added information
  */
object Manipulation {
  
  val memo = mutable.Map.empty[GridLocation, Temperature]
  
  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    /* Grader does not accept new dependencies so we need to implement a Memo pattern
    Memo.immutableHashMapMemo {
      case GridLocation(x, y) => Visualization.predictTemperature(temperatures, Location(90-y, x-180))
    }
    */
    case GridLocation(x, y) => {
      val input = GridLocation(x, y)
      if (memo.contains(input)) memo(input)
      else {
        val output = Visualization.predictTemperature(temperatures, Location(y, x))
        memo += ((input, output))
        output
      }
    }
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val yearlyGrids = temperaturess.map(makeGrid(_))
  
    (location: GridLocation) => {
      val yearlyTempsByLocation = yearlyGrids.map(f => f(location))
      yearlyTempsByLocation.reduce(_+_)/yearlyTempsByLocation.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGrid(temperatures)
  
    (location: GridLocation) => grid(location) - normals(location)
  }
}


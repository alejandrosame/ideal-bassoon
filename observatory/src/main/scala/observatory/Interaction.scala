package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val Tile(x, y, zoom) = tile

    val n = math.pow(2.0, zoom)
    val lon = x / n * 360d - 180d
    val lat = math.toDegrees(math.atan(math.sinh(math.Pi * (1 - 2*y/n))))
    
    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    
    val subtileZoom = 8
    val width = math.pow(2, subtileZoom).toInt //pixels
    val height = math.pow(2, subtileZoom).toInt  //pixels
    
    val Tile(xTile, yTile, zoomTile) = tile
    // Set starting point based on subtile zoom (pixel precision) 
    val x0 = xTile * height
    val y0 = yTile * width

    val image = new Array[Pixel](width*height) // new is necessary to allocate space 
    
    val indices = for {
      i <- 0 until width
      j <- 0 until height
    } yield (i, j)
    
    indices.par foreach {case (x, y) =>
      val location = tileLocation(Tile(x0+x, y0+y, zoomTile+subtileZoom))
      image(y*width+x) = subTile(temperatures, colors, location)
    }
    
    Image(width, height, image)
  }
  
  def subTile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], 
              location: Location): Pixel = {
    val alpha = 127
    val interpolatedColor = 
            Visualization.interpolateColor(colors, Visualization.predictTemperature(temperatures, location))
      
    Pixel(interpolatedColor.red, interpolatedColor.green, interpolatedColor.blue, alpha)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    for ((year, data) <- yearlyData) {
      for (zoom <- 0 to 3) {
        val tiles: Int = Math.pow(2, zoom).toInt
        val indices = for {
          i <- 0 until tiles
          j <- 0 until tiles
        } yield (i, j)
        
        indices.par foreach {case (x, y) =>
          generateImage(year, Tile(x, y, zoom), data)
        }
      }
    }
  }
}

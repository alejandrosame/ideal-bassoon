package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    val CellPoint(x, y) = point
    d00*(1-x)*(1-y) + d10*x*(1-y) + d01*(1-x)*y + d11*x*y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    val subtileZoom = 8
    val width = 256 //pixels
    val height = 256  //pixels
    val alpha = 127
    
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
      val location = Interaction.tileLocation(Tile(x0+x, y0+y, zoomTile+subtileZoom))
      
      val d00 = grid(GridLocation(location.lat.floor.toInt, location.lon.ceil.toInt))
      val d01 = grid(GridLocation(location.lat.ceil.toInt, location.lon.floor.toInt))
      val d10 = grid(GridLocation(location.lat.floor.toInt, location.lon.ceil.toInt))
      val d11 = grid(GridLocation(location.lat.ceil.toInt, location.lon.ceil.toInt))
      
      val cellpoint = CellPoint(location.lon - location.lon.floor.toInt, 
                                location.lat - location.lat.floor.toInt)
      val interpolatedColor = Visualization.interpolateColor(colors, 
                                                             bilinearInterpolation(cellpoint, d00, d01, d10, d11))

      image(y*width+x) = Pixel(interpolatedColor.red, interpolatedColor.green, interpolatedColor.blue, alpha)
    }
    
    Image(width, height, image)
  }
}

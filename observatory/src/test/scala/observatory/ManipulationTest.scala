package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait ManipulationTest extends FunSuite with Checkers {
  test("'makeGrid' returns a consistent grid"){
    
    //temperatures: List((Location(45.0,-90.0),5.0), (Location(-45.0,0.0),30.0)) 
    //grid: GridLocation(90,-180)
    
    val arg0Location = Location(45.0, -90.0)
    val arg1Location = Location(-45.0, 0.0)
    
    // arg0, arg1, expected
    val tempList = List( (5.0, 30.0)//,
                          //(-1.0, -80.17478327858342)
                        )

    for (temps <- tempList){
      val arg0 = temps._1
      val arg1 = temps._2
      
      val temperatures = List((arg0Location, arg0), (arg1Location, arg1)).toIterable
      
      val grid = Manipulation.makeGrid(temperatures)
            
      //given a latitude in [-89, 90] and a longitude in [-180, 179]
      for (lat <- -89 to 90){
        for (lon <- -180 to 179){ 
            
          val testLocation = GridLocation(lat, lon)
          
          val temp = grid(testLocation)
          
          // Assert only when points are not at similar distances
          if (Visualization.distance(Location(lat, lon), arg0Location) - 
              Visualization.distance(Location(lat, lon), arg1Location) > 0.01){
          
            assert(temp - arg0 < temp - arg1 == 
                   (Visualization.distance(Location(lat, lon), arg0Location) < Visualization.distance(Location(lat, lon), arg1Location)),
               s"Incorrect computed color at ${testLocation}: ${temp} ºC}. "++ 
    		 	     s"\nExpected to be closer to ${arg0} than ${arg1}")
          }
        }
      }
    }
  }
  
  test("grid values") {
    val ref = Seq(
      (Location(45.0,-90.0),20.0),
      (Location(45.0,90.0),0.0),
      (Location(0.0,0.0),10.0),
      (Location(-45.0,-90.0),0.0),
      (Location(-45.0,90.0),20.0)
    )
  
    val grid = Manipulation.makeGrid(ref)
    val gridLocation1 = GridLocation(-45,90)
    val gridLocation2 = GridLocation(88, 56)
    val gridtemp1 = grid(gridLocation1)
    val gridtemp2 = grid(gridLocation2)
    assert(gridtemp1 >= 10.0 && gridtemp1 <= 20.0)
    assert(gridtemp2 >= 5.0 && gridtemp2 <= 10.0)
    println(s"gridtemp1: $gridtemp1, gridtemp2: $gridtemp2")
  }
  
}
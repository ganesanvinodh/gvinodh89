import com.javadocmd.simplelatlng.{LatLng, util}
import com.javadocmd.simplelatlng.window.{LatLngWindow, RectangularWindow}
import com.javadocmd.simplelatlng.util.LengthUnit
import com.javadocmd.simplelatlng.LatLngTool

object GridMapper {


  def nameGrid(lat: Double, lon: Double, width: Double, unit: LengthUnit, ang: Double): (LatLngWindow[RectangularWindow], LatLng) = {
    val s = new RectangularWindow(new LatLng(lat, lon), width, unit)
    //println(s.toString)

    val rt_extreme: LatLng = LatLngTool.travel(new LatLng(lat,lon),ang,width,LengthUnit.METER)


    return (s, rt_extreme)
  }


  def getCellId(lat: Double, lon: Double): Double = {

    val start_lat: Double = 41.474937
    val start_lon: Double = -74.913585
    var window_obj_map: Map[LatLngWindow[RectangularWindow], Double] = Map()
    var lat_for_centre_500 = start_lat
    var lon_for_centre_500 = start_lon
    var ang: Double = 0.0
    //for moving vertically
    for (i <- 1 to 300) {


      //for moving horizontally
      for (a <- 1 to 300) {


        ang=90
        val rec_endpt = nameGrid(lat_for_centre_500, lon_for_centre_500, 500, LengthUnit.METER,ang)


        var cell_id: String = i + "." + a
        //println(cell_id)
        window_obj_map = window_obj_map + (rec_endpt._1 -> cell_id.toDouble)

        lat_for_centre_500 = rec_endpt._2.getLatitude
        lon_for_centre_500 = rec_endpt._2.getLongitude

      }
      ang=180
      val verti_rec_endpt = nameGrid(start_lat, start_lon, 500*i, LengthUnit.METER,ang)


      lat_for_centre_500 = verti_rec_endpt._2.getLatitude
      lon_for_centre_500 = verti_rec_endpt._2.getLongitude
    }

    for (i <- window_obj_map.keys){
      //println("map_keys: " + i.getCenter)
      //println("contains is true or false: " + i.contains(new LatLng(lat, lon)))
      if (i.contains(new LatLng(lat, lon))) {
        return window_obj_map.getOrElse(i, 0.0)
      }
    }



    return 0.0
  }


}
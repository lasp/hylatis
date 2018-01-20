package latis.reader

import latis.ops._
import latis.util.SparkUtils

/**
 * Adapter that knows how to apply image dataset generation operations 
 * to the hysics data cube.
 */
class HysicsImageAdapter(props: Map[String, String]) { //extends SparkDataFrameAdapter(props) {
  
  //Define R, G, and B wavelength values
  val r = 630.87
  val g = 531.86
  val b = 463.79
  
  //override 
  def handleOperation(op: Operation): Boolean = op match {
    case HysicsImageOp(x1,x2,y1,y2) =>
      val spark = SparkUtils.getSparkSession
      val df = spark.table("hysics")
                  .filter(s"x >= $x1")
                  .filter(s"x <  $x2")
                  .filter(s"y >= $y1")
                  .filter(s"y <  $y2")
                  .filter(s"wavelength in ($r, $g, $b)")
                  .groupBy("y","x")
                  .pivot("wavelength", Seq(r, g, b)) //improve performance by providing values
                  .sum("value") //aggregation required but not needed
                  .sort("y", "x")
                  .withColumnRenamed(r.toString, "R")
                  .withColumnRenamed(g.toString, "G")
                  .withColumnRenamed(b.toString, "B")
                  //.show
          
      df.createOrReplaceTempView("hysics_image") //TODO: use location property?
      
      true
  }
}

object HysicsImageAdapter {
  def apply() = new HysicsImageAdapter(Map("location" -> "hysics_image"))
}
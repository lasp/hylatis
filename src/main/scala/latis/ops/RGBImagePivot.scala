package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * Given a dataset of the form:
 *   (x, y, pivotVar) -> v
 * and values of the pivot variable that correspond to colors red, green, anf blue,
 * make an image dataset of the form:
 *   (x, y) -> (red, green, blue)
 * 
 */
case class RGBImagePivot(pivotVar: String, red: Double, green: Double, blue: Double) extends Operation {
  
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(tt: Tuple, v) =>
        //TODO: preserve tuple properties?
        //TODO: allow pivotVar to be other than "c"
        val (x, y) = tt.elements match { //assume flattened
          case Seq(a, b, _) => (a, b)
        }
        val r = Scalar("red")
        val g = Scalar("green")
        val b = Scalar("blue")
        Function(Metadata(), Tuple(x,y), Tuple(Metadata("id" -> "color"), r, g, b))
    }
  }

}
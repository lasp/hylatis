package latis.input

import latis.metadata._
import latis.model._
import latis.util.LatisConfig

class GoesImageReader(section: Option[String]) extends AdaptedDatasetReader with Serializable {

  def metadata: Metadata = Metadata("goes_image") //TODO: make unique, from URI?

  def model: DataType = Function(
    Tuple(
      //Note, raw values are shorts but sclae and offset are applied
      Scalar(Metadata("y") + ("type" -> "float")),
      Scalar(Metadata("x") + ("type" -> "float"))
    ),
    Scalar(
      Metadata("radiance")
      + ("type" -> "float")
      + ("origName" -> "Rad")
    )
  )

  /**
   * Defines a configuration with an optional section property.
   */
  private val config = {
    section match {
      case Some(s) => NetcdfAdapter.Config("section" -> s)
      case None => LatisConfig.get("hylatis.goes.default-section") match {
        case Some(s) => NetcdfAdapter.Config("section" -> s)
        case None    => NetcdfAdapter.Config()
      }
    }

  }

  def adapter: Adapter = new NetcdfAdapter(model, config)

}

object GoesImageReader {
  def apply():GoesImageReader = new GoesImageReader(None)
  def apply(section: String):GoesImageReader = new GoesImageReader(Some(section))
}

package latis.data

import latis.model._
import ucar.ma2.{Array => NcArray, Section}
import ucar.nc2.{Variable => NcVariable, NetcdfFile}
import fs2.Stream
import cats.effect.IO
import scala.collection.JavaConverters._
import ucar.nc2.dataset.NetcdfDataset
import latis.data.Data._

/**
 * Implement a SampledFunction that encapsulates a NetCDF dataset.
 * Capture only the variables that are represented in the given model.
 * The model must be uncurried (no nested Functions) with one-dimensional
 * domain variables and range variables with the same shape (i.e. Cartesian).
 */
case class NetcdfFunction(
  ncDataset: NetcdfDataset, 
  model: DataType, 
  section: Option[Section] = None
) extends SampledFunction {
  //TODO: ensure all range variables have the same shape
  
  /**
   * Provide a Stream of Samples from the NetcdfFile.
   */
  def streamSamples = ncStream.flatMap(readData)
  
  /**
   * Bracket the NetcdfFile in a Stream so it will manage
   * closing it.
   */
  lazy val ncStream: Stream[IO, NetcdfFile] = 
    Stream.bracket(IO(ncDataset))(nc => IO(nc.close()))
  
  /**
   * Get the name of the NetCDF variable given the identifier from the model.
   */
  def getOrigName(id: String): String = model.getVariable(id) match {
    case Some(v) => v.metadata.getProperty("origName").getOrElse(id)
    case None => ??? //TODO: error, variable not found
  }
  
  /**
   * Transform domain variable data to a DomainSet.
   */
  def ncArrayToDomainSet(arr: NcArray): DomainSet = arr.copyTo1DJavaArray match {
    //TODO: support any Data in new data branch with diff support for ordering
    case a: Array[Short]  => DomainSet(a.map(v => DomainData(ShortValue(v))))
    case a: Array[Float]  => DomainSet(a.map(v => DomainData(FloatValue(v))))
    case a: Array[Double] => DomainSet(a.map(v => DomainData(DoubleValue(v))))
  }
  
  /**
   * Define a Map of model id to the corresponding NetCDF Variable.
   * Note, this will access the file but not read data arrays.
   */
  private lazy val variableMap: Map[String, NcVariable] = {
    val ids = model.getScalars.map(_.id)
    val pairs = ids map { id =>
      (id, ncDataset.findVariable(getOrigName(id))) //TODO: error if any null, fail fast?
    }
    pairs.toMap
  }
  
  /**
   * Define the default Section based on the shape for the first range variable.
   */
  def defaultSection: Section = model match {
    //TODO: look for metadata before looking at file
    case Function(_, range) => 
      val shape = variableMap(range.getScalars.head.id).getShape
      new Section(shape)
  }
  
  /**
   * Apply the stride operation by modifying the Section and
   * returning a new NetcdfFunction.
   */
  def stride(ns: Seq[Int]): NetcdfFunction = {
    val currentSection = section getOrElse defaultSection
    val newSection = NetcdfFunction.applyStrideToSection(currentSection, ns.toArray)
    this.copy(section = Option(newSection))
    //TODO: consider shared open NetCDF file
  }
  
  /**
   * Define a function to transform a NetcdfFile into a Stream of Samples.
   */
  val readData: NetcdfFile => Stream[IO, Sample] = (netcdfFile: NetcdfFile) => {
    //TODO: seems a shame to build a memoized SetFunction just to make a Stream
    //TODO: read chunks or slices by outer dimension
    
    val (domainArrays, rangeArrays) = model match {
      case Function(domain, range) => (
        domain.getScalars.map(_.id).map(readVar),
        range.getScalars.map(_.id).map(readVar)
      )
    }
    
    val domainSet = ProductSet(domainArrays.map(ncArrayToDomainSet): _*)
    
    val totalLength = rangeArrays.head.getSize.toInt
    
    val rangeValues = for {
      index <- 0 until totalLength
    } yield RangeData(rangeArrays.map(a => Data(a.getObject(index))): _*)
      
    SetFunction(domainSet, rangeValues).streamSamples
  }

  /**
   * Read the NetCDF variable with the given model id.
   */
  def readVar(id: String): NcArray = {
    //val ncvar = ncDataset.findVariable(getOrigName(id)) //TODO: handle error, not found
    val ncvar = variableMap(id) //TODO: handle error, not found
    val fullSection = section getOrElse defaultSection
    // For domain variables, extract the nc Range for that dimension.
    val path = model.getPath(id).get //TODO: or else error, not found
    val varSection = path.head match { //assumes no nesting, uncurried
      case DomainPosition(i) => new Section(fullSection.getRange(i))
      case _ => fullSection
    }
    ncvar.read(varSection)
  }

    
  /**
   * Consider this SampledFunction empty if all the dimensions
   * in the NetCDF file have zero length. Presumably, an empty
   * NetCDF file would return an empty list of Dimensions.
   */
  def isEmpty: Boolean =
    ncDataset.getDimensions.asScala.forall(_.getLength == 0)
    //TODO: limit to vars in the model
}


object NetcdfFunction {
  
  def applyStrideToSection(section: Section, stride: Array[Int]): Section = {
    //TODO: check rank
    val origin = section.getOrigin
    val size = section.getShape zip section.getStride map {
      case (shape, stride) => shape * stride
    }
    val newStride = section.getStride zip stride map {
      case (s1, s2) => s1 * s2
    }
    new Section(origin, size, newStride)
  }
}


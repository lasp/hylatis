package latis.data

import latis.model._
import ucar.ma2.{Array => NcArray, Section}
import ucar.nc2.{Variable => NcVariable, NetcdfFile}
import fs2.Stream
import cats.effect.IO
import scala.collection.JavaConverters._
import latis.util.StreamUtils
import latis.data.Data.DoubleValue
import ucar.nc2.dataset.NetcdfDataset

/**
 * Implement a SampledFunction that encapsulates a NetCDF file.
 * Capture only the variables that are represented in the given model.
 */
case class NetcdfFunction(
  ncDataset: NetcdfDataset, 
  model: DataType, 
  section: Option[Section] = None
) extends SampledFunction {
  // Assume model is uncurried: (x, y, z) -> (a, b, c)
  // Assume domain variable are 1D coordinate variables (Cartesian)
  //TODO: ensure all range variables have the same shape
  //TODO: use enhanced NetcdfDataset? understands valid range, missing, scale, offset
  
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
  
  
  /*
   * TODO: apply operations
   * make new NetcdfFunction with diff model?
   * use "section" metadata? for selections
   *   can't modify model here, use config?
   * projections supported via simply reading what is in the model
   * 
   * Operation modifies model eagerly
   *   this can't modify the model
   *   construct with sections for each variable?
   *   config? consider tsml atts to specify subsets
   *     try to be true to data file, apply subset as ops
   *     even removing a dimension via evaluation?
   *     easy to specify section
   *     can use indices
   * manage range for each domain variable
   *   as constructor arg?
   *   make new instance with application of operation   
   * 
   * Config by variable?
   * just another dot (".") layer with id?
   *   time.section? or section.time? section: Map id => value  
   */
  
  /**
   * Get the name of the NetCDF variable given the identifier from the model.
   */
  def getOrigName(id: String): String = model.getVariable(id) match {
    case Some(v) => v.metadata.getProperty("origName").getOrElse(id)
    case None => ??? //TODO: error, variable not found
  }


  def readVar(id: String): NcArray = {
    val ncvar = ncDataset.findVariable(getOrigName(id))
  //TODO: apply ops via a section
  val origin = Array(0,0)
  val shape = ncvar.getShape
  val stride = Array(2712,2712)  
    
    val section = new Section(origin, shape, stride)
    ncvar.read(section)
  }
  
  
  def ncArrayToDomainSet(arr: NcArray): DomainSet = arr.copyTo1DJavaArray match {
    //TODO: support any Data in new data branch
    case a: Array[Short] => DomainSet(a.map(v => DomainData(DoubleValue(v.toDouble))))
    case a: Array[Float] => DomainSet(a.map(v => DomainData(DoubleValue(v.toDouble))))
    case a: Array[Double] => DomainSet(a.map(v => DomainData(DoubleValue(v))))
  }
  
  private val variableMap: Map[String, NcVariable] = {
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
    case Function(_, range) => 
      val shape = variableMap(range.getScalars.head.id).getShape
      new Section(shape)
  }
  
  def stride(ns: Int*): NetcdfFunction = {
    val newSection = section match {
      case Some(sect) =>
        NetcdfFunction.applyStrideToSection(sect, ns.toArray)
      case None => defaultSection
    }
    this.copy(section = Option(newSection))
  }
  
  /**
   * Define a function to transform a NetcdfFile into Samples.
   */
  val readData: NetcdfFile => Stream[IO, Sample] = (netcdfFile: NetcdfFile) => {
    
    val (domainArrays, rangeArrays) = model match {
      case Function(domain, range) => (
        domain.getScalars.map(_.id).map(readVar),
        range.getScalars.map(_.id).map(readVar)
      )
    }
    
    val totalLength = rangeArrays.head.getSize.toInt
    
    val domainSet = ProductSet(domainArrays.map(ncArrayToDomainSet): _*)
    
    val rangeValues = for {
      index <- 0 until totalLength
    } yield RangeData(rangeArrays.map(a => Data(a.getObject(index))): _*)
      
    SetFunction(domainSet, rangeValues).streamSamples
  }
  /*
   * TODO: seems a shame to build a memoized SetFunction just to make a Stream
   * TODO: read chunks or slices by outer dimension
   * 
   */

    
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
    val shape  = section.getShape 
//    zip stride map {
//      case (a, b) => a / b
//    }
    val newStride = section.getStride zip stride map {
      case (a, b) => a * b
    }
    new Section(origin, shape, newStride)
  }
}


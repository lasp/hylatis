package latis.data

import latis.model._
import ucar.ma2.{Array => NArray, Section}
import ucar.nc2.NetcdfFile
import fs2.Stream
import cats.effect.IO
import scala.collection.JavaConverters._
import latis.util.StreamUtils

/**
 * Implement a SampledFunction that encapsulates a NetCDF file.
 */
case class NetcdfFunction(ncFile: NetcdfFile, model: DataType) extends MemoizedFunction {
  // Assume model is uncurried: (x, y, z) -> (a, b, c)
  // Assume domain variable are 1D
  //TODO: ensure all range variables have the same shape
  //Assuming  domain is just indices, for now
  
  /*
   * use origName
   *   can't use the same chars in our IDs
   */
  def getOrigName(id: String): String = model.findVariable(id) match {
    case Some(v) => v.metadata.getProperty("origName").getOrElse(id)
    case None => ??? //TODO: error, variable not found
  }

  
  def readVar(varName: String): NArray = {
    val ncvar = ncFile.findVariable(varName)
    val section = new Section(ncvar.getShape) //TODO: apply stride and other ops, lazily
    ncvar.read(section)
  }
  
  
  //def streamSamples: Stream[IO, Sample] = {
  def samples: Seq[Sample] = {
    val ncArrs: Seq[NArray] = model match {
      case Function(_, range) => 
        range.getScalars.map(_.id).map(getOrigName).map(readVar)
      case _ => ??? //TODO: assuming array
    }
    // Shape should be the same for all range variables
    val shape: Array[Int] = ncArrs.head.getShape
    val n = ncArrs.head.getSize.toInt
    
    /*
     * need to construct nD domain tuple so we can make samples to stream
     * or should we make DomainSet? hold all in memory, we do it anyway
     *   unless we get more clever about buffering reads
     *   e.g. iterate over outer dimension, apply to shape, could curry
     * product set
     * could we get this from nc Arrays? getIndex? might account for stride
     */
    val domains: Seq[DomainData] = shape.length match {
      case 1 => Vector.tabulate(shape(0))(DomainData(_))
      case 2 => Vector.tabulate(shape(0), shape(1))(DomainData(_,_)).flatten
      case 3 => Vector.tabulate(shape(0), shape(1), shape(2))(DomainData(_,_,_)).flatten.flatten
      case _ => ??? //TODO: support other ranks
    }

    val samples = for {
      index <- (0 until n)
      domain = domains(index)
      range = RangeData(ncArrs.map(a => Data(a.getObject(index))): _*)
    } yield Sample(domain, range)

    //StreamUtils.seqToIOStream(samples)
    samples
  }
    
//  /**
//   * Consider this SampledFunction empty if all the dimensions
//   * in the NetCDF file have zero length. Presumably, an empty
//   * NetCDF file would return an empty list of Dimensions.
//   */
//  def isEmpty: Boolean =
//    ncFile.getDimensions.asScala.forall(_.getLength == 0)
}
package latis.input

import java.net.URI
import java.nio.file._

import cats.effect.IO
import fs2.Stream
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.{Variable => NcVariable}
import ucar.nc2.dataset.NetcdfDataset

import latis.data._
import latis.model._
import latis.ops.Operation
import latis.ops.Stride
import latis.util._

/**
 * Experimental modification of the NetcdfAdapter to support
 * MODIS quirks.
 *  - section of one variable, no coord vars
 */
case class ModisNetcdfAdapter(
  model: DataType,
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {

  /**
   * Specifies which operations that this adapter will handle.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case _: Stride => true
    //case Stride(stride) if (stride.length == model.arity) => true
    //TODO: domain selections, take, drop, ...
    case _ => false
  }

  /**
   * Reads the data for the modeled variables and section
   * as defined by the operations.
   * The data will be read into memory as a SetFunction.
   */
  def getData(uri: URI, ops: Seq[Operation]): SampledFunction = {
    import collection.JavaConverters._
    // Get default section from URI and put in config
    val config = uri.getFragment match {
      case null => NetcdfAdapter.Config()
      case s    => NetcdfAdapter.Config("section" -> s)
    }
    // Get variable name from URI and reconstruct file URI
    val (fileURI, vname) = {
      val ss = uri.toString.split("!/")
      (new URI(ss(0)), ss(1).split("#")(0))
    }

    val ncStream: Stream[IO, NetcdfDataset] = NetcdfAdapter.open(fileURI)
    ncStream.map { ncDataset =>
      val nc = ModisNetcdfWrapper(ncDataset, model, config)

      // Applies ops to default section
      val section: Section = nc.applyOperations(ops)

      // Assumes no coordinate variables, use indices
      //// Assumes all domain variables are 1D and define a Cartesian Product set.
      //val domainSet: DomainSet = model match {
      //  case Function(domain, _) =>
      //    val dsets: List[DomainSet] = domain.getScalars.zipWithIndex.map {
      //      case (scalar, index) =>
      //        val sec   = new Section(section.getRange(index))
      //        val ncArr = nc.readVariable(scalar.id, sec)
      //        val ds: IndexedSeq[DomainData] =
      //          (0 until ncArr.getSize.toInt).map { i =>
      //            Data.fromValue(ncArr.getObject(i)) match {
      //              case Right(d: Datum) => DomainData(d)
      //              case Left(le)        => throw le //TODO: error or drop?
      //            }
      //          }
      //        SeqSet1D(scalar, ds)
      //    }
      //    if (dsets.length == 1) dsets.head
      //    else ProductSet(dsets)
      //}

      // Get domain dimensions
      // Note, need to get from var, ":" results in null range
      // ignoring the 1st dim
      //val shape = ncDataset.findVariable(vname).getShape
      //val shape = section.getShape
      //val strides = section.getStride
      val ranges: Array[URange] = section.getRanges.asScala.toArray
      val r1 = ranges(1)
      val r2 = ranges(2)
      val domainSet = LinearSet2D(
        LinearSet1D(r1.first, r1.stride, r1.length),
        LinearSet1D(r2.first, r2.stride, r2.length)
      )

      // Note, all range variables must have the same shape
      // consistent with the domain set
      val rangeData: IndexedSeq[RangeData] = model match {
        case Function(_, range) =>
          //// Read the NcArray for each range variable
          //val arrs: List[NcArray] = range.getScalars.map { scalar =>
          //  nc.readVariable(scalar.id, section)
          //}
          // Assume only one range variable
          val arrs: List[NcArray] = List(nc.readVariable(vname, section))
          (0 until arrs.head.getSize.toInt).map { i =>
            RangeData(arrs.map { a =>
              Data.fromValue(a.getObject(i)) match {
                case Right(d) => d
                case Left(le) => throw le //TODO: error or fill?
              }
            })
          }
      }

      SetFunction(domainSet, rangeData)
    }.compile.toVector.unsafeRunSync().head //Note, will close file

  }

}


/**
 * Defines a wrapper for a NetCDF dataset that provides convenient access
 * and applies operations by modifying the requested Section.
 */
case class ModisNetcdfWrapper(ncDataset: NetcdfDataset, model: DataType, config: NetcdfAdapter.Config) {

  ///**
  // * Defines a Map of LaTiS variable id to the corresponding NetCDF Variable.
  // * Note, this will access the file but not read data arrays.
  // */
  //private lazy val variableMap: Map[String, NcVariable] = {
  //  //TODO: fail faster by not making this lazy?
  //  val ids = model.getScalars.map(_.id)
  //  val pairs = ids.map { id =>
  //    val vname = getNcVarName(id)
  //    ncDataset.findVariable(vname) match {
  //      case v: NcVariable => (id, v)
  //      case null =>
  //        val msg = s"NetCDF variable not found: $vname"
  //        throw LatisException(msg)
  //    }
  //  }
  //  pairs.toMap
  //}

  /**
   * Gets the section as defined in the config or else
   * makes a section for the entire dataset.
   */
  def defaultSection: Section = config.section match {
    case Some(spec) => new Section(spec) //TODO: error handling
    case None       =>
      // Complete Section, ":" for each dimension
      val spec = List.fill(model.arity)(":").mkString(",")
      new Section(spec)
  }

  /**
   * Applies the given operations to define the final section to read.
   */
  def applyOperations(ops: Seq[Operation]): Section =
    ops.foldLeft(defaultSection)(NetcdfAdapter.applyOperation)


  //// Note, get is safe since the id comes from the model in the first place
  //private def getNcVarName(id: String): String =
  //  model.findVariable(id).get.metadata.getProperty("origName").getOrElse(id)

  /**
   * Reads the section of the given variable into a NcArray.
   * This is where the actual IO is done.
   */
  def readVariable(id: String, section: Section): NcArray =
    //variableMap(id).read(section)
    ncDataset.findVariable(id).read(section)

  //def close(): Unit = ncDataset.close() //ncStream.compile.drain.unsafeRunSync()
}

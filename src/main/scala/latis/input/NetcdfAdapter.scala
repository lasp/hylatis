package latis.input

import java.net.URI
import java.nio.file._

import ucar.ma2.{Array => NcArray}
import ucar.ma2.Section
import ucar.nc2.{Variable => NcVariable}
import ucar.nc2.dataset.NetcdfDataset

import latis.data._
import latis.model._
import latis.ops.Operation
import latis.util.AWSUtils
import latis.util.ConfigLike
import latis.util.LatisConfig
import latis.util.LatisException

case class NetcdfAdapter(
  model: DataType, 
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {

  override def canHandleOperation(op: Operation): Boolean = false


  /*
  TODO: all needs to be triggered by call to getData(uri)
    so don't manage any state about the ncfile in the adapter
    encapsulate in NetcdfHelper?
    would like to reuse variable Map
    avoid clutter within getData
   */
  def getData(uri: URI, ops: Seq[Operation]): SampledFunction = {

    val nc = NetcdfWrapper(open(uri), model, config)

    /*
    TODO: build Section from ops, only need 2d stride for goes
      start with optional section in config
      for GOES for now
     */
    val section = {
      val spec = LatisConfig.get("hylatis.goes.default-section").getOrElse {
        ??? //TODO: default to all
      }
      new Section(spec)
    }
    //TODO: put in NetcdfAdapter.Config
    //TODO: default section
    //val section = new Section(s"(0:5423:10, 0:5423:10)")
    //val section = new Section(s"(1000:4500:10, 1000:4500:10)")

    // Assumes all domain variables are 1D and define a Cartesian Product set.
    val domainSet: DomainSet = model match {
      case Function(domain, _) =>
        val dsets: List[DomainSet] = domain.getScalars.zipWithIndex.map {
          case (scalar, index) =>
            val sec = new Section(section.getRange(index))
            val ncArr = nc.readVariable(scalar.id, sec)
            val ds: IndexedSeq[DomainData] = (0 until ncArr.getSize.toInt).map { i =>
              DomainData(Data(ncArr.getObject(i)))
            }
            DomainSet(ds)
        }
        if (dsets.length == 1) dsets.head
        else ProductSet(dsets)
    }

    // Note, all range variables must have the same shape
    // consistent with the domain set
    val rangeData: IndexedSeq[RangeData] = model match {
      case Function(_, range) =>
        val arrs: List[NcArray] = range.getScalars.map {
          scalar => nc.readVariable(scalar.id, section)
        }
        (0 until arrs.head.getSize.toInt).map { i =>
          RangeData(arrs.map(a => Data(a.getObject(i))))
        }
    }

    nc.close() //no longer need data file

    SetFunction(domainSet, rangeData)
  }

  /**
   * Returns a NetcdfDataset from the given URI.
   * NetcdfDataset vs NetcdfFile: metadata conventions applied to data values.
   *   applies scale_factor and add_offset
   *   applies valid_range and _FillValue resulting in NaN
   *   TODO: is data type always double?
   *     does missing_value end up as NaN?
   */
  def open(uri: URI): NetcdfDataset = {
    //TODO: resource management, make sure this gets closed
    //lazy val ncStream: Stream[IO, NetcdfDataset] =
    //  Stream.bracket(IO(ncDataset))(nc => IO(nc.close()))
    uri.getScheme match {
      case null => 
        NetcdfDataset.openDataset(uri.getPath) //assume file path
      case "s3" => 
        // Create a local file name
        val (bucket, key) = AWSUtils.parseS3URI(uri)
        val dir = LatisConfig.get("file.cache.dir") match {
          case Some(dir) => dir
          case None => Files.createTempDirectory("latis").toString
        }
        val file = Paths.get(dir, bucket, key).toFile
        // If the file does not exist, make a local copy
        //TODO: deal with concurrency
        if (! file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
        NetcdfDataset.openDataset(file.toString)
      case "file" => 
        NetcdfDataset.openDataset(uri.getPath)
      case _    =>
        NetcdfDataset.openDataset(uri.getScheme + "://" + uri.getHost + "/" + uri.getPath)
    }
  }

}


object NetcdfAdapter {
  
  case class Config(properties: (String, String)*) extends ConfigLike {
    val section: Option[Section] = get("section").map(new Section(_))
  }
}

case class NetcdfWrapper(ncDataset: NetcdfDataset, model: DataType, config: NetcdfAdapter.Config) {

  /**
   * Defines a Map of LaTiS variable id to the corresponding NetCDF Variable.
   * Note, this will access the file but not read data arrays.
   */
  private lazy val variableMap: Map[String, NcVariable] = {
    //TODO: fail faster by not making this lazy?
    val ids = model.getScalars.map(_.id)
    val pairs = ids map { id =>
      val vname = getNcVarName(id)
      ncDataset.findVariable(vname) match {
        case v: NcVariable => (id, v)
        case null =>
          val msg = s"NetCDF variable not found: $vname"
          throw LatisException(msg)
      }
    }
    pairs.toMap
  }

  // Note, get is safe since the id comes from the model in the first place
  private def getNcVarName(id: String): String =
    model.findVariable(id).get.metadata.getProperty("origName").getOrElse(id)

  //def getVariableShape(id: String): Array[Int] =
  //  variableMap(id).getShape

  def readVariable(id: String, section: Section): NcArray =
    variableMap(id).read(section)

  def close(): Unit = ncDataset.close()
}

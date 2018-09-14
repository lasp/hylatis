package latis.input

import latis.model._
import latis.data._
import latis.metadata._
import java.net.URI
import java.io._


case class AsciiAdapter(config: AsciiAdapter.Config, val model: DataType)
  extends IterativeAdapter[String] {
  //TODO: TextAdapter? Tabular? (vs matrix, columnar)
  /*
   * TODO: StreamSource
   * AsciiAdapters need an InputStrem
   * how to add S3
   * construct with a stream source?
   * UriResolver
   *   javax.xml.transform.URIResolver.resolve(href, base): Source
   *   e.g. StreamSource.getInputStream, getReader
   *   other subclasses of Source
   *   
   * registry?
   * latis property: resolver.s3.class
   * s3.getObject("hylatis-hysics-001", "des_veg_cloud/wavelength.txt").getObjectContent
   * 
   * S3 URIs
   * can we use http: http://bucket.s3.amazonaws.com/key
   *   http://hylatis-hysics-001.s3.amazonaws.com/des_veg_cloud/wavelength.txt
   *   403, need to add policies to bucket
   * s3://bucket/key
   *   not standard but used by some
   *   used by the netcdf s3 reader!
   */
  
  /**
   * Manage source InputStream
   * TODO: auto closable 
   */
  private var inputStream: InputStream = null
  
  def close(): Unit = if (inputStream != null) inputStream.close() //TODO error handling
  
  /**
   * Keep track of whether we have encountered a data marker.
   */
  private var foundDataMarker = false


  //---- Parse operations -----------------------------------------------------

  /**
   * Return an Iterator of data records. Group multiple lines of text for each record.
   */
  def recordIterator(uri: URI): Iterator[String] = {
    var rs = getLineIterator(uri)
    rs = config.linesPerRecord match {
      case 1 => rs
      case n => rs.grouped(n).map(_.mkString(config.delimiter))
    }
    
    //TODO: apply length of Function if given
    //TODO: handle limit, take,... ops
    config.limit match {
      case -1 => rs
      case n  => rs.take(n)
    }
  }
  
  /**
   * Return Iterator of lines, filter out lines deemed unworthy by "shouldSkipLine".
   */
  def getLineIterator(uri: URI): Iterator[String] = {
    //TODO: support S3
    inputStream = ???//getStream(uri)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    import collection.JavaConverters._
    val lines: Iterator[String] = reader.lines.iterator.asScala
    lines.drop(config.skip).filterNot(shouldSkipLine(_))
  }
  
  /**
   * This method will be used by the lineIterator to skip lines from the data source
   * that we don't want in the data. 
   * Note that the "isEmpty" test bypasses an end of file problem iterating over the 
   * iterator from Source.getLines.
   */
  def shouldSkipLine(line: String): Boolean = {
    if (config.marker == null || foundDataMarker) {
      // default behavior: ignore empty lines and lines that start with comment characters
      line.isEmpty() || (config.commentCharacter != null && line.startsWith(config.commentCharacter))
    } else {
      // We have a data marker and we haven't found it yet,
      // therefore we should ignore everything until we
      // find it. We should also exclude the data marker itself
      // when we find it. 
      if (line.matches(config.marker)) foundDataMarker = true;
      true
    }
  }
  
  
  /**
   * Return Map with Variable name to value(s) as Strings.
   */
  def parseRecord(record: String): Option[Sample] = {
    //TODO: handle projections- reduce number of values to parse; selections?
    /*
     * TODO: consider nested functions
     * if not flattened, lines per record will be length of inner Function (assume cartesian?)
     * deal with here or use algebra?
     */
    //assume one value per scalar per record
    //TODO: assert length is the same as number of scalars
    //assume model is uncurried function (or scalar or tuple), then we can handle multi-dimensional dataset
  //  val nd = model.arity
    val vs = extractValues(record) //string value for each scalar
      
    /*
     * traverse model and build Sample
     * use foreach?
     * pull off value as we hit a scalar
     * how might this work with nested functions in ascii tables?
     * 
     * could we make Data with strings then parse later?
     * pattern match extractor?
     * seems dangerous
     */
    
    
    //Sample(ds.take(nd), ds.drop(nd))
    ???
  }
  
  /**
   * Extract the Variable values from the given record.
   */
  def extractValues(record: String): Seq[String] = splitAtDelim(record)
  
  def splitAtDelim(str: String): Array[String] = str.trim.split(config.delimiter, -1)
  //Note, use "-1" so trailing ","s will yield empty strings.
  //TODO: StringUtil?
  

//  def makeDatum(scalar: Scalar, record: Map[String,String]): Option[Datum] = {
//    val value = record(scalar.id)
//    scalar match {
//      //TODO: stringToDatum util?
//      case _: Integer => Some(Datum(value.toLong)) //TODO: handle error
//      case _: Real    => Some(Datum(value.toDouble)) //TODO: handle error
//      case _: Text    => Some(Datum(value)) //TODO: impose length? trim?
//    }
//  }

}

//=============================================================================

object AsciiAdapter {
  //TODO: TextAdapter

  case class Config(
    commentCharacter: String = null,
    delimiter: String = ",",
    linesPerRecord: Int = 1,
    skip: Int = 0,
    marker: String = null,
    limit: Int = -1
  ) 
  
}

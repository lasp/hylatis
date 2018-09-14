package latis.output

import latis.data._
import latis.metadata._
import latis.model._
import latis.util._
import java.io._
import scala.collection._

//NOTE: we don't have ownership of output stream so don't close
class Writer(out: OutputStream) {
  //TODO: subclass for OutputStream Writers? doesn't apply to all destinations (spark, database)
  //TODO: add TextWriter hierarchy with PrintWriter
  /*
   * TODO: separate out the OutputStream
   * need to return "data" that can be sunk into an OutputStream
   * akin to Reader/Adapter
   * Writer does IO and delegates
   * diff layer to output something streamable
   * text output: Iterator[String]
   */
  
  private lazy val printWriter = new PrintWriter(out)
  
  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset): Unit = {
    printWriter.println(dataset) //header
    val f = (s: Sample) => writeSample(dataset.model, s)
    dataset.samples.foreach(f)
    printWriter.flush()
  }
  
  def writeSample(model: DataType, sample: Sample): Unit = {
    val s = sampleToString(model, sample)
    printWriter.println(s)
  }
  
  def sampleToString(model: DataType, sample: Sample): String = model match {
    case Function(domain, range) =>
      varToString(domain, sample.domain) + " -> " + varToString(range, sample.range)
    case _ => varToString(model, sample.range)
  }
  
  def varToString(model: DataType, data: Data): String = (model, data) match {
    case (t: Scalar, d: ScalarData[_])  => scalarToString(t, d)
    case (t: Tuple, d: TupleData)       => tupleToString(t, d)
    case (t: Function, d: SampledFunction) => functionToString(t, d)
  }
  
  def scalarToString(stype: Scalar, scalar: ScalarData[_]): String = scalar match {
    // Type does not matter for string conversion
    //TODO: apply precision...
    case ScalarData(v) => v.toString
    case _ => ??? //empty data, get fill value from metadata?
    //TODO: delegate to custom type? scalar.toString(data)?
  }
  
  def tupleToString(ttype: Tuple, tuple: TupleData): String = {
    //(ttype.flatten.toSeq zip tuple.elements).map(p => varToString(p._1, p._2)).mkString("(", ", ", ")")
    //assume no nested tuple type
    (ttype.elements zip tuple.elements).map(p => varToString(p._1, p._2)).mkString("(", ", ", ")")
  }
  
  /**
   * Platform independent new-line.
   */
  val newLine = System.getProperty("line.separator") //"\n"
  //TODO: put in StringUtils?
  
  //nested function
  //TODO: indent
  def functionToString(ftype: Function, function: SampledFunction): String = {
    function.samples.map(sampleToString(ftype, _))
      .mkString(s"{$newLine", newLine, s"}$newLine")
  }

}

//==== Companion Object =======================================================

object Writer {
  
  def apply(): Writer = Writer(System.out)
  
  def apply(out: OutputStream): Writer = new Writer(out)
  
  //TODO: fileName, file
}

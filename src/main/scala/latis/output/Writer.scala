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
 val z = dataset.samples.next
    val f = (s: Sample) => writeSample(dataset.model, s)
    dataset.samples.foreach(f)
    printWriter.flush()
  }
  
  def writeSample(model: DataType, sample: Sample): Unit = {
    val s = sampleToString(model, sample)
    printWriter.println(s)
  }
  
  /*
   * TODO: new Sample: (DomainData, RangeData)
   * effectively zip model with data
   * need to preserve model, pull data as needed, recursive? fold?
   * 
   * scalars should be matched with Any and functions with SampledFunction
   * can we define an efficient function to apply: Sample => String
   *   to avoid all the flattening and matching for each sample?
   */
  def sampleToString(model: DataType, sample: Sample): String = (model, sample) match {
    case (Function(domain, range), Sample(ds, rs)) =>
      dataToString(domain, ds) + " -> " + dataToString(range, rs)
  }
  
  //recurse with string buffer accumulator
  /*
   * TODO: factor out recursion? use DataType as traversable? 
   * f: DataType => U
   * embed data (decumulator) and string buffer (accumulator)
   * or in scope of outer function that wraps foreach call
   * 
   * how to add header/footer? e.g. tuple ()
   * 
   * can't simply match and recurse, need to pull from stack
   *   e.g. start at function: range tuple must pick off what it needs, no way to proceed with other data
   */
  def dataToString(model: DataType, data: Seq[_]): String = {
    //val sb = new java.lang.StringBuilder() //Note: java StringBuilder is leaner/faster but not thread safe
    val ds = mutable.Stack(data: _*)
    
    def go(dt: DataType): String = dt match {
      //TODO: error if ds is empty
      case _: Scalar => ds.pop.toString  //TODO factor out scalarToString?
      case Tuple(es @ _*) => es.map(go(_)).mkString("(", ",", ")")
        
      case ft @ Function(d, r) => ds.pop match {
        case sf: SampledFunction => functionToString(ft, sf)
        case _ => ??? //Oops, model and data not consistent
      }
    }
    
    go(model)
    //sb.toString
  }
  
//  def varToString(model: DataType, data: Any*): String = {
//    val sb = new java.lang.StringBuilder() //Note: java StringBuilder is leaner/faster but not thread safe
//    val ds = mutable.Stack(data)
//    
//    val f = (model: DataType) => model match {
//      case _: Scalar => sb append ds.pop
//      case Tuple(es @ _*) => es
//    }
//    
//    model foreach f
//    
////    (model, data) match {
////    case (dt: Scalar, h::Nil)  => scalarToString(dt, h) //Scalar is a leaf so there better be only one data value left
////    case (t: Tuple, ds)       => tupleToString(t, d)
////    case (t: Function, d: SampledFunction) => functionToString(t, d)
//    sb.toString
//  }
  
//  def scalarToString(stype: Scalar, data: Any): String = scalar match {
//    // Type does not matter for string conversion
//    //TODO: apply precision...
//    case ScalarData(v) => v.toString
//    case _ => ??? //empty data, get fill value from metadata?
//    //TODO: delegate to custom type? scalar.toString(data)?
//  }
  
//  def tupleToString(ttype: Tuple, tuple: TupleData): String = {
//    //(ttype.flatten.toSeq zip tuple.elements).map(p => varToString(p._1, p._2)).mkString("(", ", ", ")")
//    //assume no nested tuple type
//    (ttype.elements zip tuple.elements).map(p => varToString(p._1, p._2)).mkString("(", ", ", ")")
//  }
  
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

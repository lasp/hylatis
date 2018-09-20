package latis.ops

import latis.model._
import latis.metadata._
import latis.data._
import scala.collection.mutable.ArrayBuffer

/**
 * Given a Dataset with potentially nested Functions, undo the nesting.
 * For example:
 *   a -> b -> c  =>  (a, b) -> c
 *   
 * Assumes Cartesian Product domain set (e.g. same set of bs for each a).
 */
case class Uncurry() extends Operation {
  
  override def applyToModel(model: DataType): DataType = {
    //TODO: assume no Functions in Tuples, for now
    //TODO: neglect function and tuple IDs, for now; append with "."?
    def go(dt: DataType, ds: List[DataType], rs: List[DataType]): DataType = dt match {
      case s: Scalar => 
        val range = Tuple(Metadata(), (rs :+ s): _*)  //TODO: scalar if length is 1, or wait till the end?
        if (ds.isEmpty) range
        else Function(Metadata(), Tuple(Metadata(), ds: _*), range) 
      case t: Tuple => 
        val range = Tuple(Metadata(), t.elements.map(go(_, List.empty, List.empty)): _*)
        if (ds.isEmpty) range
        else Function(Metadata(), Tuple(Metadata(), ds: _*), range)
      case Function(d, r) => Function(Metadata(), Tuple(Metadata(), (ds :+ d): _*), go(r, List.empty, List.empty)) //TODO: flatten tuple, or wait till the end?
      //TODO: add Function and Tuple smart constructors with no Metadata?
      //TODO: 
    }
    
    /*
     * build up from bottom vs top
     * recursion, catamorphism
     * building up an accumulator feels top down
     * does this call for top-down?
     *   seems like it
     *   function in range needs to have its domain merged with outer function domain
     * 
     * start with pair of (ds,rs), return that pair instead of building a DataType
     * fold?
     * traversable
     */
//    def go2(dt: DataType, ps: (List[DataType], List[DataType])): (List[DataType], List[DataType]) = (dt, ps) match {
//      case (s: ScalarType, (ds,rs)) => (ds, rs :+ s)
//      //case (TupleType(es @ _*), (ds, rs)) => (ds, rs :+ s)
//      case (FunctionType(d,r), (ds,rs)) => (ds :+ d, go())
//    }
    
    val ds = ArrayBuffer[DataType]()
    val rs = ArrayBuffer[DataType]()
    
    def go3(dt: DataType): Unit = dt match {
      case dt: Scalar => rs += dt
      //case Tuple(es) => es.foreach(go3)
      case t: Tuple => t.elements.foreach(go3)
      case Function(d, r) => 
        ds += d //TODO: flatten tuples, or later
        go3(r)
    }
    
    //TODO: can we use a fold?  avoid unwrap and wrap
    //note: DataType is traversable
    //go(model, List.empty, List.empty)
    go3(model)
    val rtype = rs.length match {
      case 1 => rs.head
      case _ => Tuple(Metadata(), rs: _*)
    }
    ds.length match {
      case 0 => rtype
      case 1 => Function(Metadata(), ds.head, rtype)
      case _ => Function(Metadata(), Tuple(Metadata(), ds: _*), rtype) //TODO: flatten domain, Traversable builder not working
    }
  }
  

  /*
   * TODO: nested Function will make Seq[Samples]
   *  can't use MapOperation?
   *  make a function to flatMap over RDD
   */
  // 
  def makeMapFunction(model: DataType): Sample => Seq[Sample] = {
    //TODO: Iterator?
    (s: Sample) => s match {
      case Sample(ds, rs) => 
        //TODO: more convenient extraction? Sample(ds, rs)?
        //TODO: recurse for deeper nested functions
        //TODO: allow function in tuple
        rs.head match {
          case SampledFunction(samples) => samples.toSeq map {
            case Sample(ds2, rs2) =>
              Sample(ds ++ ds2, rs2)
          }
          case _ => Seq(s) //no-op if range is not a Function
        }
    }
  }
    
  override def applyToData(ds: Dataset): SampledFunction = {
    val f = makeMapFunction(ds.model)
    val samples = ds.samples.flatMap(f)
    //TODO: replicate orig Function impl?
    StreamingFunction(samples)
  }
}

package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

case class Select(vname: String, operator: String, value: String) extends Filter {
  //TODO: error if var not found?
  //TODO: support DAP2  a={1,2,3} for "contains" semantics
  
  def makePredicate(model: DataType): Sample => Boolean = {
    /*
     * TODO: quickly indicate location of desired value in sample
     * Map in Sample would be handy but bloaty
     * rather not "find" in the model for every sample
     * some encoding for location in data model?
     * eval Sample with it to get value?
     * (a, b, c) -> (d, e, f -> g)
     *  b: d1
     *  d: r0
     *  g: r2r0
     * x -> y -> z
     *  z: r0r0
     * but have to parse
     * could just do 0,1,2...
     *   consider getScalars
     *   Function Data does not know type
     *   can't use position into function data without getting a sample
     *   generally ok since nested functions shouldn't be traversableOnce
     *   
     * or make a function: Sample -> Data  (lens?)
     * for nested function pull out "column" just for that value
     * general capability to pull given var data
     * akin to old getDoubleMap
     *   but just for one sample
     * do by name, could be a tuple, return Seq[Data]
     * could this be used by projections?
     * get Data vs testing each with predicate
     *   short circuit
     *   Iterator.find?
     *   nestedFunction.samples recurse with diff index
     * 
     * consider using index/bin search
     *  
     * nested functions
     * all or none
     * use "find" or "contains"
     * if one nested sample matches, keep the whole nested function?
     * AND vs OR issue
     * can we offer either?
     *   ">>"
     *   "!<"
     */
    
    /*
     * do he straight forward thing then consider encoding the variable position
     * zip model with sample
     *   can we define FDM structure and use it for DataType and Data (up to but not into Function data)?
     *   FDM[DataType] zip FDM[Data] => FDM[(DataType, Data)]
     *   A: DataType, Data, Metadata?
     *   Scalar[A], Tuple[A], Function[A] ?
     *     but Tuple[A] contains FDM[A]s, not just As
     *     
     *   don't need to iterate into Function for this
     *   nested Function has atomic Data or Metadata
     * can zip any two iterables
     * 
     * filter via traversable
     * 
     */
    
    
    /*
     * TODO: match alias: findByName
     * indexOf?
     * model.toSeq results in all ScalarTypes but breaks into Function
     * worry about nested functions later
     * assume all are scalars
     */
    val index: Int = model.getScalars.indexWhere(_.id == vname)
    //TODO: Use model to make Scalar Data for the given "value"
    //  assume double values for now
    
    (sample: Sample) => sample match {
      case Sample(ds, rs) =>
        val vs = ds ++ rs
        val c = ScalarOrdering.compare(vs(index), value)  //assumes uncurried
        isValid(c)
    }
  }
  
  
  private def isValid(comparison: Int): Boolean = {
    if (operator == "!=") {
      comparison != 0
    } else {
      (comparison < 0  && operator.contains("<")) || 
      (comparison > 0  && operator.contains(">")) || 
      (comparison == 0 && operator.contains("="))
    }
  }
  
}


object Select {
  
  def apply(expression: String): Select = {
    //TODO: beef up expression parsing
    val ss = expression.split("\\s+") //split on whitespace
    Select(ss(0), ss(1), ss(2))
  }
}
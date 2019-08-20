package latis.ops

import latis.data._
import latis.model._
import latis.metadata.Metadata

/**
 * Given a dataset of the form:
 *   pivotVar -> (x, y) -> a
 * and values of the pivot variable that correspond to colors red, green, and blue,
 * make an image dataset of the form:
 *   (x, y) -> (r, g, b)
 * This assumes that every x-y domain set is the same (outer Function is Cartesian).
 */
//case class RGBImagePivot(pivotVar: String, red: Double, green: Double, blue: Double) extends UnaryOperation {
case class RGBImagePivot(red: Double, green: Double, blue: Double) extends UnaryOperation {
  

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    // Evaluate cube for each color
    val grids = Vector(red, green, blue).map { v =>
      data(DomainData(v)) match {
        case Some(RangeData(mf: MemoizedFunction)) => mf //(x, y) -> a
        //case Some(RangeData(sf: SampledFunction)) => sf.unsafeForce //(x, y) -> a
        case _ => ??? //TODO: error
      }
    }
    
    grids.reduce(join)
  }
  
  private def join(sf1: MemoizedFunction, sf2: MemoizedFunction): MemoizedFunction = {
    val samples = (sf1.samples zip sf2.samples) map { 
      // Assume same domain
      case (Sample(d, r1), Sample(_, r2)) => Sample(d, r1 ++ r2)
    }
    
    SampledFunction.fromSeq(samples)
  }
  
  override def applyToModel(model: DataType): DataType = model match {
    case Function(_, Function(domain, _)) => Function(
      domain, 
      Tuple(
        Scalar(Metadata("id" -> "r", "type" -> "double")), 
        Scalar(Metadata("id" -> "g", "type" -> "double")), 
        Scalar(Metadata("id" -> "b", "type" -> "double"))
      )
    )
  }
  
  
  // Assume (w, ix, iy) -> f
//  override def apply(ds: Dataset): Dataset = {
//    val ops = Seq(
//      Contains(pivotVar, red, green, blue)
//      , GroupBy("ix", "iy")
//      , Pivot(Vector(red, green, blue), Vector("r","g","b"))  //TODO: evaluate w -> f
//    )
//    
//    ops.foldLeft(ds)((ds, op) => op(ds))
//  }
  
  /*
   * TODO: break into basic ops
   * starts with (iy, ix, w) -> f
   * filter ws
   * groupBy + transpose + agg => (ix, iy) -> w -> f
   * pivot?
   *   note, spectrum all in memory, but not always
   *   useless to pivot on outer domain: 1 row, n cols
   *     maybe for matrix but I wouldn't call that "pivot"
   *   do we even need to name pivot var? 
   *     always do right-most function?
   *     
   *   assume all spectra have same set of ws
   *     require Cartesian trait?
   *     how could we check and fill?
   *     eval each spectrum for the same domain set?
   *       FillInterpolation
   * 
   * Curry: like groupBy but no need to shuffle?
   *   (x,y,w) -> f  =>  (x,y) -> w -> f
   */
  /*
   * note: transpose in domain requires shuffling
   * uncurry does not need reshuffle
   * streamable implies no shuffling
   * don't need to use spark groupBy?
   *   just map f then partition?
   *   but need to aggregate SampledFunction 
   *   use PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey?
   *   presumably sparks's groupby might manage all this more efficiently?
   *   
   * do we need a groupBy in the FA or just use map...?
   *   can only do with MemoizedFunction since we have to sort
   *     could memoize in the process, unsafe
   *   need more than map
   *   can't impl in Op if we want to delegate to RddFunction
   *   do we need to uncurry first?
   *     otherwise can't get data out of nested tuple
   *     only if var is nested, determine by path
   *     
   *  a -> (b,c)
   *  groupBy(b): => b -> a -> c
   *  map?: (a;b,c) => (b;F(a->c))
   *    map not sufficient, need to gather all b values (orig samples without b) then aggregate into SampledFUnction
   *    use SortedMap: b -> SortedSet[S(a;c)]
   *      or faster to sort at the end?
   *    only after going through all samples can we agg SortedSet into SampledFunction
   *  Use SampledFunction impl that uses this Map?
   *  See latis2 Factorization
   */

  //      case RGBImagePivot(vid, r, g, b) =>
//        val colors = Set(r,g,b)
//        val colorFilter: Sample => Boolean = (sample: Sample) => sample match {
//          case (DomainData(_, _, w: Double), _) =>
//            if (colors.contains(w)) true else false
//        }
//        
//        val groupByFunction: Sample => DomainData = (sample: Sample) => sample match {
//          //assume first 2 vars in domain are row, col
//          case Sample(ds, _) => DomainData.fromSeq(ds.take(2))  
//        }
//        
//        val agg: (DomainData, Iterable[Sample]) => Sample = (data: DomainData, samples: Iterable[Sample]) => {
//          //samples should be spectral samples at the same pixel
//          //after groupBy: (row, column); (row, column, w) -> f
//          val ss = samples.toVector map {
//            case Sample(ds, rs) => Sample(ds.drop(2), rs)  // w -> f
//          }
//          val stream: Stream[IO, Sample] = Stream.eval(IO(ss)).flatMap(Stream.emits(_))
//          (data, RangeData(StreamFunction(stream))) // (row, column) -> w -> f
//        }
//          
//        val pivot: Sample => Sample = (sample: Sample) => sample match {
//          // (row, column) -> w -> f  =>  (row, column) -> (r, g, b)
//          // assumes 3 wavelength values have been previously selected
//          case (domain, RangeData(SampledFunction(ss))) =>
//            // Create (r,g,b) tuple from spectrum
//            // pivot  w -> f  =>  (r, g, b)
//            val colors = ss.compile.toVector.unsafeRunSync() map {
//              case (_, RangeData(d)) => d
//            }
//            
//            (domain, RangeData(colors: _*))  // (row, column) -> (r, g, b)
//        }
//        
//        implicit val ord = DomainOrdering
//        rdd = rdd.filter(colorFilter)
//                 .groupBy(groupByFunction)  //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
//                 .map(p => agg(p._1, p._2))
//                 .map(pivot) //TODO: can we use spark pivot?
//                 .sortBy(_._1) //(DomainOrdering) 
  
//  override def applyToModel(model: DataType): DataType = {
//    model match {
//      case Function(tt: Tuple, v) =>
//        //TODO: preserve tuple properties?
//        //TODO: allow pivotVar to be other than "c"
//        val (x, y) = tt.elements match { //assume flattened
//          case Seq(a, b, _) => (a, b)
//        }
//        val r = Scalar("red")
//        val g = Scalar("green")
//        val b = Scalar("blue")
//        Function(Metadata(), Tuple(x,y), Tuple(Metadata("id" -> "color"), r, g, b))
//    }
//  }

}
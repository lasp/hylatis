package datasets

import latis.metadata._
import latis.reader._
import latis.ops._
import latis.model._

//object hysics_image extends DatasetDescriptor(
//    Metadata("id" -> "hysics_image")
//  )(
//    Function(Metadata("id" -> "image"))(
//      Tuple(Integer("y"), Integer("x")),
//      Tuple(Real("R"), Real("G"), Real("B"))
//    )
////    Function(Metadata("id" -> "cube"))(
////      Tuple(Integer(id = "x"), Integer(id = "y"), Real(id = "wavelength")),
////      Real(id = "value")
////    )
//  )(
//    SparkDataFrameAdapter("hysics")
//  )(
//    Operations(
//      /*
//       * TODO: we want to apply x,y selections before others 
//       * but they are input by the user
//       * have user supply the makeImage(r,g,b) op?
//       * CompositeOperation = Seq[Operation] ?
//       */
//  )
//)
/*
 * TODO: extend some form of DatasetSource
 * access "hysics" data frame
 * apply rgb subset/pivot (as Operation?)
 * use SparkDataFrameAdapter to gather result
 * write image
 * 
 * local dataset backed by spark via SparkDataFrameAdapter
 * assume "hysics" is already in spark
 * use DSL to define "hysics" dataset with SparkDataFrameAdapter
 * apply ops to this local dataset which delegates to spark via SparkDataFrameAdapter
 * note, new ds needs new name that matches new (tmp?) data frame
 * consider user inputs for x,y range
 * 
 * operation needs to be given a dataset
 * but an adapter can impl it any way it wants
 */




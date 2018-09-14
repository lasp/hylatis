package latis.input

import latis.model._
import latis.ops._
import latis.util.LatisProperties

trait DatasetSource {
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[Operation]): Dataset
  def getDataset(): Dataset = getDataset(Seq.empty)
  
}

object DatasetSource {
  
  /**
   * Get dataset descriptor defined with DSL.
   */
  def fromName(datasetName: String): DatasetSource = {
    //Look for a matching "reader" property.
    LatisProperties.get(s"reader.${datasetName}.class") match {
      case Some(s) =>
        Class.forName(s).getConstructor().newInstance().asInstanceOf[DatasetSource]
    }
    
//    //TODO: add other sources: tsml. lemr, properties
//    //import scala.reflect.runtime.currentMirror
//    val ru = scala.reflect.runtime.universe
//    val mirror = ru.runtimeMirror(getClass.getClassLoader)
//    
//    val cls = LatisProperties.getOrElse("dataset.dir", "datasets") + "." + dsid
//    
//    val moduleSymbol = mirror.classSymbol(Class.forName(cls)).companion.asModule
//    mirror.reflectModule(moduleSymbol).instance.asInstanceOf[DatasetDescriptor]
  }
}
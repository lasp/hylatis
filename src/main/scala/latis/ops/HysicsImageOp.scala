package latis.ops

import latis.model.Dataset

case class HysicsImageOp(x1: Int, x2: Int, y1: Int, y2: Int) extends Operation {
  
  def apply(dataset: Dataset): Dataset = {
    
    ???
  }
}

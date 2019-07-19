package latis.util

import latis.data._

/**
 * Define an invertible CoordinateSystemTransform 
 * in terms of to and from functions between DomainData.
 */
case class CoordinateSystemTransform(
  to: DomainData => DomainData,
  from: DomainData => DomainData
) {
  
  /**
   * Enable this CoordinateSystemTransform to be used as a function.
   */
  def apply(data: DomainData): DomainData = to(data)
  
  /**
   * Create a CoordinateSystemTransform that is an inverse of this one.
   */
  def inverse = CoordinateSystemTransform(this.from, this.to)
}
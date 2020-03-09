package latis.util

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.data._

class TestPartitioner extends JUnitSuite {
  
  val domainSet = BinSet1D(1.0, -5.0, 10)
  val domainSetPartitioner = DomainSetPartitioner(domainSet)
  
  @Test
  def partition_count() = 
    assertEquals(11, domainSetPartitioner.numPartitions)
    
  @Test
  def min() = {
    val i = domainSetPartitioner.getPartition(DomainData(-5.0))
    assertEquals(0, i)
  }
  
  @Test
  def max() = {
    val i = domainSetPartitioner.getPartition(DomainData(5.0))
    assertEquals(10, i)
  }
  
  @Test
  def max_valid() = {
    val i = domainSetPartitioner.getPartition(DomainData(4.9))
    assertEquals(9, i)
  }
  
  @Test
  def boundary() = {
    /*
     * TODO: DO we want bin-centered behavior?
     * indexOf
     * goes back to construction options
     * [min, max) ?
     *   count intuition often off by 1
     * add min and max methods to DomainSet?
     *   max exclusive, odd for "range" but ok for domain coverage
     *   
     */
    println(domainSet.min, domainSet.max)
    println(domainSet.indexOf(DomainData(-5.50001))) // -1
    println(domainSet.indexOf(DomainData(4.5))) // -1
    println(domainSet.indexOf(DomainData(4.4999))) // 9
    val i = domainSetPartitioner.getPartition(DomainData(0.0))
    assertEquals(4, i)
  }

  @Test
  def match_tuple() = {
    val p = HylatisPartitioner(10, 0.0, 10)
    val d = DomainData(3.3, -2)
    assertEquals(3, p.getPartition(d))
  }
}

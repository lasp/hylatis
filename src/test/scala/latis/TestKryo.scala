package latis

import latis.data._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import latis.data.Data.DoubleValue

case class Foo(d: Double)

object TestKryo extends App {
  val kryo = new Kryo()
  
  //val o = Sample(DomainData(1), RangeData(2)) //181, 505
  //val o = (Vector(1), Vector(2)) //153, 529
  //val o = (Array(1), Array(2)) //13, 110
  //val o = (List(1), List(2)) //87, 297
  //val o = Sample(DomainData(1.0), RangeData(2.0)) //198, 516
  //val o = Sample(DomainData(1, 1.0), RangeData(2, 2.0)) //228, 577
  //val o = Sample(DomainData(1, 1.0, "1"), RangeData(2, 2.0, "1")) //263, 663  with arrays: 160, 344
  //val o = (Array(1, 1.0, "1"), Array(2, 2.0, "1")) //56, 282
  //val o = (Vector(Data(1), Data(1.0), Data("1")), Vector(Data(2), Data(2.0), Data("1"))) //263, 663
  //val o = (List(Data(1), Data(1.0), Data("1")), List(Data(2), Data(2.0), Data("1"))) //213, 435
  //val o = (Array(Data(1), Data(1.0), Data("1")), Array(Data(2), Data(2.0), Data("1"))) //145, 343
  //val o = (Array(Data(1), Data(1.0), Data("1")).toSeq, Array(Data(2), Data(2.0), Data("1")).toSeq) //WrappedArray 198, 491
  //val o = (3, Array(Data(1), Data(1.0), Data("1"), Data(2), Data(2.0), Data("1"))) //143, 410
  //val o = (3, Array(1, 1.0, "1", 2, 2.0,"1")) //54, 282
  //val o = (3, List(1, 1.0, "1", 2, 2.0,"1")) //122, 371
  //val o = (3, Vector(1, 1.0, "1", 2, 2.0,"1")) //126, 541
  
  val o = DoubleValue(1.0) //9, 64
  //val o = Data(1.0) //9, 64
  //val o = Array(Data(1.0)) //40, 103
  //val o = Foo(1.0) //9, 42

  val output = new Output(new FileOutputStream("kryo.bin"))
  kryo.writeObject(output, o)
  output.close()
  
  val oos = new ObjectOutputStream(new FileOutputStream("java.bin"))
  oos.writeObject(o)
  oos.close()
}

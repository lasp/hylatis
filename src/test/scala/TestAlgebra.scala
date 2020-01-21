trait TestAlgebra[A] {
  def curry(arity: Int): A
  def project(p: String): A
}

trait Smp //Sample
trait M //Model
trait S { //Stream[IO, Sample]
  def map(f: Smp => Smp): S
  def filter(f: Smp => Boolean): S
}
//TODO: take impl out of SF to make trait that can be in sealed Data ADT
trait F { //extends TestAlgebra[F] { //SampledFunction
  def stream: S
  //def curry(arity: Int): F = F(C(arity).pipe(stream))
  //def project(p: String): F = F(P(p).pipe((stream)))
  /*
  TODO: but makes another copy of the Op, no access to model
   should SF know the model?
   could SF take Op? don't need pretty DSL for SF
     otherwise SF would need model
       tempting
       but would also need to construct an Op with the model
   How to override?
     pattern match to take the ones it can, delegate others to super?
     match MapOp, Filter vs specific?
     can we trust smart F?
       allow only white list to override?
       do like Adapter: offer only the ones they say they can handle
         canHandleOp(op): Boolean
         trust still not enforced
   Summary: alg on SF has no way to get model
     Only need DSL for Dataset, not SF
     Let Op define the impl
     Let SF apply Op
       stream by default
       override as needed
     DatasetOps defines the algebra
       no need for separate trait
         unless something else could implement it?
       constructs the Op
   */
  def op(o: Op): F = o.atD(this)
}
object F {
  //TODO: construct with implicit Algebra vs extend
  def apply(s: S): F = new F { def stream = s }
}

class RddF extends F {
  def stream: S = ???
  //override def curry(arity: Int): RddF = ???

  //override def project(p: String): RddF = ???
  //otherwise get default via stream
  override def op(o: Op): F = o match {
    case c: C => new RddF
    case _ => super.op(o)
  }
}

case class DS(m: M, f: F) {
  val ops: Seq[Op] = ???
  def withOp(op: Op): DS = ??? //new DS with Op
  def applyOps: DS = {
    //compile...
    //fold: see TappedDataset
    val m2 = ops.head.atM(m)
    val f2 = ops.head.atD(f)
    DS(m2, f2)
  }
}

object DS {
  implicit class DsOps(ds: DS) extends TestAlgebra[DS] {
    def curry(arity: Int): DS = {
      val op = C(arity, ds.m)
      ds.withOp(op)
    }
    def project(p: String): DS = ???
  }
}

trait Op {
  def prov = ""
  def atM(m: M): M
  def atD(f: F): F
}
case class C(arity: Int, m: M) extends StreamOp {
  def atM(m: M): M = ??? //applyToModel
  //def atD(f: F): F = ??? //applyToData
  def pipe: S => S = ???
}

trait StreamOp extends Op {
  def pipe: S => S
  def atD(f: F): F = F(pipe(f.stream))
}
trait MapOp extends StreamOp {
  def mapF: Smp => Smp
  def pipe: S => S = (s: S) => s.map(mapF)
}
trait Filter extends StreamOp {
  def pred: Smp => Boolean
  def pipe: S => S = (s: S) => s.filter(pred)
}

case class P(exp: String) extends Filter {
  def atM(m: M): M = ??? //applyToModel
  def pred: Smp => Boolean = ???
}

object TestAlgebra extends App {
  val s: S = ???
  val m: M = ???
  val f = F(s)
  val ds = DS(m, f)
    .curry(2)
    .project("foo")
}

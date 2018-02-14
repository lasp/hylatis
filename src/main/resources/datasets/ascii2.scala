package datasets

import latis.metadata._
import latis.reader._
import ImplicitConversions._
import latis.ops._
import latis.model._

object ImplicitConversions {
  implicit def nameToMetadata(id: String): Metadata = Metadata("id" -> id)
}

/*
 * latis.dsl.Dataset?
 * DatasetDescriptor?
 *   extends DatasetSource
 *   getDataset
 *   no better than "dataset" member
 * as opposed to IS-A Dataset
 *   DslDataset extends Dataset?
 *   but whole thing is a DSL
 *   args won't map
 *   need smart constructor
 *  
 * Dataset with ops
 * adapter.makeDataset(md, model, ops)
 * *return dataset with unapplied ops!
 * when to apply them?
 * have adapter apply then just before returning?
 * thought about collecting them before, need to apply eagerly to model?
 *   e.g. in repl, rename then show
 * certain class of Ops can be held? ops on Samples
 * throw dataset with ops to spark to apply them?
 * 
 * As DatasetSource via fromName
 * need to be able to request "ascii" in URL
 * DatasetSource.fromName("ascii")
 * so we need a source to hold it so might as well have it BE the source
 * 
 */



//object ascii2 extends DatasetDescriptor(
//  "ascii2"
//)(
//  Function("f")(
//    Text(id="myTime"),
//    Tuple(
//      Integer(id="myInt"),
//      Real(id="myReal"),
//      Text(id="myText")
//    )
//  )
//)(
//  AsciiAdapter(
//    //"location" -> "file:/Users/lindholm/git/latis3/latis-core/src/test/resources/data/mixed.txt",
//    location = "file:/home/lindholm/git/latis3/latis-core/src/test/resources/data/mixed.txt"
//  , delimiter = """\s+"""
//  , comment_character = "#"
//  )
//)(   
////    Operations(
////      Select("x > 1"),
////      First
////    )
//)

//object ascii {
//  val dataset = Dataset("ascii")(
//    Function("f")(
//      Text(id="myTime"),
//      Tuple(
//        Integer(id="myInt"),
//        Real(id="myReal"),
//        Text(id="myText")
//      )
//    ),
//    
//    operations = Seq[Operation](
////      Select("x > 1"),
////      First
//    ),
//    
//    adapter = AsciiAdapter(
//      //"location" -> "file:/Users/lindholm/git/latis3/latis-core/src/test/resources/data/mixed.txt",
//      location = "file:/home/lindholm/git/latis3/latis-core/src/test/resources/data/mixed.txt"
//    , delimiter = """\s+"""
//    , comment_character = "#"
//    )
//  )
//}
  
/*
 * TODO: construct Dataset with Adapter
 * DSL replacement for tsml
 * seems like adapter (and ops) need to be outside the scope of dataset?
 * model vs complete dataset
 * but dataset not complete until adapter "attaches" data and ops are applied
 * use a DatasetSource to bring it all together
 *   model + adapter + ops (+ user ops)
 *   DatasetSource (reader):
 *     makes model
 *     constructs adapter with model
 *     asks adapter for completed dataset, with given ops
 *   location/URL/URI: java class name
 * 
 * Could Dataset have an adapter?
 * source as part of its provenance
 * but each transformation would need to carry it on (unless memoized - or is the adapter just to the memoized copy?)
 * what about a netcdf reader: given url, make model, use adapter
 *   close reader? already know we can't
 *   problematic to hold onto source, try/catch/finally
 * model as Dataset, completed as Dataset with Data (mixin or new subclass)?
 * 
 * this object as proxy for the dataset, like tsml
 * thus extend Dataset
 * use dataset smart constructor 
 *   make it private to the dataset package?
 * but how to close/release resources?
 *   close Dataset? but could be used after memoized
 *   "force" does memoize and close?
 *   MemoizedDataset?   
 *   clever FP pattern for managing resources?
 *     but don't know when user is done
 *     except in context of server response
 *     write: end of the universe
 *     close then unless memoized
 *   close after out of scope?
 *   finalize on adapter?
 * server request still needs to go through DatasetSource to map name to object
 * any direct use for dataset object?
 *   repl dsl?
 * 
 * instead of adapter injecting stuff into a Dataset then turning it loose
 * let the Dataset own the adapter (or "source"?)
 * 
 * DatasetRegistry instead of DatasetSource?
 *   no "close" semantics
 *   Dataset.fromName?
 *   how to pass ops?
 *   can apply ops at any time before writing
 *     it has the adapter right there to optimize them
 *   provide an opportunity to cache after PIs
 *   
 * Reader constructed with URL
 *   e.g. tsml, which c/should be closed
 *   netcdf: needs to read metadata then could close
 *   lemr,... just sources of metadata, proxy to data
 * just use "Reader" to match Writer?
 * or Source/Sink?
 * 
 * Use adapter only at end of world?
 * no close concerns
 * what do we need to know when?
 *   effect of projection on ascii columns?
 *     include column number in scalar md?
 *     or just info in ascii adapter: id => col
 *   should adapter have basic API for getting values by id?
 *     getNext?
 *     ok if we memoize in scalar?
 *     effectively still iterating over samples
 *     
 * offer each op for handling?
 * consider optimizations
 *   jdbc, netcdf: 
 */
  



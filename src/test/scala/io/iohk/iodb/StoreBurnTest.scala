package io.iohk.iodb

import java.io.File
import java.util.Random
import org.junit._
import org.scalatest.Assertions

/**
  * Tests store under continuous usage (disk leaks, data corruption etc..
  */
class StoreBurnTest extends TestWithTempDir {

  var store:Store = null;

  /** run time for each test in seconds */
  val time = 60

  @Before override def init() {
    super.init()
    store = new TrivialStore(dir=dir, keySize =32, keepLastN = 10)
  }

  def storeSize:Long = dir.listFiles().map(_.length()).sum

  /** tests for disk leaks under continous usage */
  @Test def continous_insert(){
    val endTime = System.currentTimeMillis()+ time*1000

    val random = new Random()
    var keys = Seq.empty[ByteArrayWrapper]

    var version = 1
    while(System.currentTimeMillis()<endTime){
      keys.foreach { it =>
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i=>TestUtils.randomA())
      val toUpdate = newKeys.map(a=>(a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)
      version+=1
      if(version%5==0)
        store.clean()

      keys = newKeys
      //check for disk leaks
      assert( storeSize < 100*1024*1024)
    }
  }

  @Test def continous_rollback(){
    val endTime = System.currentTimeMillis()+ time*1000

    val random = new Random()
    var keys = Seq.empty[ByteArrayWrapper]
    var rollbackKeys = Seq.empty[ByteArrayWrapper]

    var version = 1

    while(System.currentTimeMillis()<endTime){
      keys.foreach { it =>
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i=>TestUtils.randomA())
      val toUpdate = newKeys.map(a=>(a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)

      if(version==50) {
        rollbackKeys = newKeys
      }

      version+=1

      keys = newKeys
      if(version>100) {
        store.rollback(50)
        version = 51
        keys = rollbackKeys
      }

      //check for disk leaks
      assert( storeSize < 100*1024*1024)
    }
  }

}

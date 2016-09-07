package examples

import java.io.File

import io.iohk.iodb.ByteArrayWrapper
import io.iohk.iodb.skiplist.AuthSkipList
import org.junit.Test
import org.mapdb._


/**
  * Demonstrates Authenticated Skip List
  */
class SkipList {


  /**
    * Create AuthSkipList with onheap store. It is fastest option in-memory option.
    * No serialization is done, SL nodes are stored on Java heap, and affected by garbage collector.
    * It has scalability issues, practical limit is 10M entries or 4 GB RAM.
    */
  @Test def initialize_on_heap(): Unit = {
    val store = DBMaker
      .heapDB() //store type
      .make().getStore() //construct DB and get its store
    val skiplist = AuthSkipList.createEmpty(store = store, keySize = 32)

    //insert key and value into SL
    skiplist.put(new ByteArrayWrapper(32), new ByteArrayWrapper(7))
  }


  /**
    * Create AuthSkipList with in-memory store. It is slower than onheap, but more space efficient.
    * It also scales linearly and can handle several hundred GBs.
    * SL nodes are serialized and stored in large byte arrays.
    */
  @Test def initialize_in_memory(): Unit = {
    val store = DBMaker
      .memoryDB() //store type
      .make().getStore() //construct DB and get its store
    val skiplist = AuthSkipList.createEmpty(store = store, keySize = 32)

    //insert key and value into SL
    skiplist.put(new ByteArrayWrapper(32), new ByteArrayWrapper(7))
  }


  /**
    * Create AuthSkipList backed by memory mapped file.
    * Skip List nodes are serialized and stored in mapped ByteBuffers
    */
  @Test def initialize_file(): Unit = {

    // create file, in this case temp
    val file = File.createTempFile("temp", "store")
    file.delete() // file should not exist, when new store is created

    val store = DBMaker
      .fileDB(file) //store type
      .fileMmapEnable() // enable memory mapped files (faster file access)
      .make().getStore() //construct DB and get its store
    val skiplist = AuthSkipList.createEmpty(store = store, keySize = 32)

    //insert key and value into SL
    skiplist.put(new ByteArrayWrapper(32), new ByteArrayWrapper(7))

    // at end file storage needs to be closed
    store.close()


    //now reopen closed Skip List

    //head recid is needed as parameter to reopen Skip List
    val headRecid = skiplist.headRecid
    val store2 = DBMaker.fileDB(file).fileMmapEnable().make().getStore()
    val skiplist2 = new AuthSkipList(
      store = store2,
      headRecid = headRecid,
      keySize = 32)

    //get an value from Skip List
    val value2 = skiplist2.get(new ByteArrayWrapper(32))
    assert(value2.size == 7)
  }


  /**
    * Shows howto get Root Hash and Proof of Existence,
    */
  @Test def proof_of_existence(): Unit = {
    val store = DBMaker.memoryDB().make().getStore()
    val skiplist = AuthSkipList.createEmpty(store = store, keySize = 8)

    // insert some values into Skip List
    import io.iohk.iodb.TestUtils.fromLong
    //use function to convert Long into Array[Byte]
    for (key <- (0L until 100).map(fromLong)) {
      skiplist.put(key, key)
    }

    // get Root Hash
    val rootHash = skiplist.rootHash()

    //get value associated with Key
    val key = fromLong(12L)
    val value = skiplist.get(key)

    //get search path for given key
    val path = skiplist.getPath(key)

    //compare hash path with the root hash
    assert(rootHash == path.rootHash())
  }


}

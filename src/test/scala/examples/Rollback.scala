package examples

import java.io.File

import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import org.junit.Test

/**
  * Shows how rollback is performed
  */
class Rollback {

  @Test def rollback() {

    //create temporary dir
    val dir = File.createTempFile("iodb", "iodb")
    dir.delete()
    dir.mkdir()

    //open new store
    val store = new LSMStore(dir = dir, keySize = 8)

    //insert first update
    val firstVersionID = ByteArrayWrapper.fromLong(11)
    store.update(versionID = firstVersionID, toRemove = Nil,
      toUpdate = List((ByteArrayWrapper.fromLong(11), ByteArrayWrapper.fromLong(11))))

    //insert second update
    val secondVersionID = ByteArrayWrapper.fromLong(22)
    store.update(versionID = secondVersionID, toRemove = Nil,
      toUpdate = List((ByteArrayWrapper.fromLong(22), ByteArrayWrapper.fromLong(22))))

    // rollback to first version, second version will be discarded
    store.rollback(firstVersionID)
  }

}

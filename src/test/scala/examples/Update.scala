package examples

import java.io.File

import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import org.junit.Test

/**
  * Simple example which shows how updates are performed
  */
class Update {

  @Test def update() {

    //create temporary dir
    val dir = File.createTempFile("iodb", "iodb")
    dir.delete()
    dir.mkdir()

    //open new store
    val store = new LSMStore(dir = dir)

    // VersionID identifies snapshot created by update.
    // It can be latter used for rollback.
    val versionID = ByteArrayWrapper.fromLong(11)

    // Update is done in batch, so create list of entries to insert.
    // Key and values are `byte[]` wrapped in `ByteArrayWrapper`.
    // List of pairs, first entry is key, second entry is value.
    val toUpdate: Seq[(ByteArrayWrapper, ByteArrayWrapper)] = (1 until 1000)
      .map(ByteArrayWrapper.fromLong(_))
      .map(key => (key, key))

    // List of keys to delete in update.
    val toRemove: Seq[ByteArrayWrapper] = Nil

    store.update(versionID = versionID, toRemove = toRemove, toUpdate = toUpdate)

  }

}

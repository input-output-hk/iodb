package io.iohk.iodb

import org.junit.Test


class QuickStoreRefTest extends StoreTest {
  override def open(keySize: Int): Store = new QuickStore(dir)

  @org.junit.Ignore
  @Test override def longRunningUpdates(): Unit = {

  }

}
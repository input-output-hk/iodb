package io.iohk.iodb


class QuickStoreRefTest extends StoreTest {
  override def open(keySize: Int): Store = new QuickStore(dir)


}
package io.iohk.iodb.skiplist

import java.io.PrintStream

import com.google.common.primitives.Bytes
import io.iohk.iodb.ByteArrayWrapper
import org.mapdb._
import scorex.crypto.hash.{Blake2b256, CryptographicHash}

import scala.collection.mutable


object AuthSkipList{
  type K = ByteArrayWrapper
  type V = ByteArrayWrapper
  type Recid = Long
  type Hash = ByteArrayWrapper

  def defaultHasher = Blake2b256;

  /** Level for each key is not determined by probability, but from key hash to make Skip List structure deterministic.
    * Probability is simulated by checking if hash is dividable by a number without remainder (N % probability == 0).
    * At each level divisor increases exponentially.
    */
  protected[skiplist] def levelFromKey(key:K):Int={
    var propability = 3
    val maxLevel = 10
    val hash = key.hashCode
    for(level <- 0 to maxLevel){
      if(hash%propability!=0)
        return level
      propability = propability*propability
    }
    return maxLevel
  }

  def createEmpty(store:Store, keySize:Int, hasher:CryptographicHash = defaultHasher):AuthSkipList = {
    //insert empty head
    val ser = new TowerSerializer(keySize=keySize, hashSize = hasher.DigestSize)
    val headTower = new Tower(
      key = new ByteArrayWrapper(keySize),
      value=new ByteArrayWrapper(0),
      right=Nil,
      hashes=Nil
    )
    val headRecid = store.put(headTower, ser)
    new AuthSkipList(store=store, headRecid = headRecid, keySize=keySize, hasher=hasher)
  }

  def createFrom(source:Iterable[(K,V)], store:Store, keySize:Int, hasher:CryptographicHash = defaultHasher): AuthSkipList = {
    def emptyHash:Hash = new ByteArrayWrapper(hasher.DigestSize)
    val towerSer = new TowerSerializer(keySize=keySize,  hashSize = hasher.DigestSize)
    var rightRecids = mutable.ArrayBuffer(0L) // this is used to store links to already saved towers. Size==Max Level
    var prevKey:K = null; //used for assertion
    for((key, value)<-source) {
      //assertions
      assert(prevKey==null|| prevKey.compareTo(key)>0, "source not sorted in ascending order")
      prevKey = key

      val level = levelFromKey(key)     //new tower will this number of right links

      //grow to the size of `level`
      while(level+1>=rightRecids.size){
        rightRecids+=0L
      }
      //cut first `level` element for tower
      val towerRight = rightRecids.take(level+1).toList
      assert(towerRight.size==level+1)

      //construct tower
      val tower = new Tower(key=key, value=value,
        right = towerRight,
        hashes = (0 until level+1).map(a=> emptyHash).toList
      )

      val recid = store.put(tower, towerSer)

      //fill with  links to this tower
      rightRecids(level) = recid
      (0 until level).foreach(rightRecids(_)=0)

      assert(rightRecids.size>=level)
    }

    //construct head
    val head = new Tower(
      key=new ByteArrayWrapper(keySize),
      value=new ByteArrayWrapper(0),
      right = rightRecids.toList,
      hashes = rightRecids.map(r=> emptyHash).toList
    )
    val headRecid = store.put(head, towerSer)
    return new AuthSkipList(store=store,  headRecid=headRecid, keySize=keySize,hasher=hasher)
  }


}
import AuthSkipList._

/**
  * Authenticated Skip List implemented on top of MapDB Store
  */
class AuthSkipList(
                    protected val store:Store,
                    protected[skiplist] val headRecid:Long,
                    protected val keySize:Int,
                    protected val hasher:CryptographicHash = AuthSkipList.defaultHasher
) {

  protected[skiplist] val towerSerializer = new TowerSerializer(keySize, hasher.DigestSize)

  protected[skiplist] def loadTower(recid:Long): Tower ={
    if(recid==0) null
    else store.get(recid, towerSerializer)
  }
  protected[skiplist] def loadHead() = store.get(headRecid, towerSerializer)


  def close() = store.close()

  def get(key:K):V = {
    var node = loadHead()
    var level = node.right.size-1
    while(node!=null && level>=0) {
      //load node, might be null
      val rightTower = loadTower(node.right(level))
      //try to progress left
      val compare = if(rightTower==null) -1  else key.compareTo(rightTower.key)
        if(compare==0) {
          //found match, return value from right node
          return rightTower.value
        }else if(compare>0){
          //key on right is smaller, move right
          node = rightTower
        }else{
          //key on right is bigger or non-existent, progress down
          level-=1
      }
    }
    return null
  }


  def put(key: K, value:V): Boolean = {
    ??? //TODO
  }

  /** traverses skiplist and prints it structure */
  def printStructure(out:PrintStream = System.out): Unit ={
    out.println("=== SkipList ===")

    def printRecur(tower:Tower): Unit ={
      out.println("  "+tower)
      for(recid<-tower.right.filter(_!=0)) {
        printRecur(loadTower(recid))
      }
    }

    printRecur(loadHead())
  }


  /** calculates average level for each key */
  protected[skiplist] def calculateAverageLevel(): Double ={
    val root = loadHead()
    var sum = 0L
    var count = 0L

    def recur(tower:Tower): Unit ={
      count+=1
      sum += tower.right.size
      for(recid<-tower.right.filter(_!=0)) {
        recur(tower)
      }
    }
    return 1D * sum/count
  }
}

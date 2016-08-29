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
      value = new ByteArrayWrapper(0),
      right = List(0L),
      hashes = List(new ByteArrayWrapper(hasher.DigestSize))
    )
    val headRecid = store.put(headTower, ser)
    new AuthSkipList(store=store, headRecid = headRecid, keySize=keySize, hasher=hasher)
  }

  def createFrom(source:Iterable[(K,V)], store:Store, keySize:Int, hasher:CryptographicHash = defaultHasher): AuthSkipList = {
    def emptyHash:Hash = new ByteArrayWrapper(hasher.DigestSize)

    def hashKey(key:K): Hash ={
      return  new ByteArrayWrapper(hasher.hash(key.data))
    }

    def hashNode(hash1:Hash, hash2:Hash):Hash = {
      assert(hash1.size==hasher.DigestSize)
      assert(hash2.size==hasher.DigestSize)
      val joined = Bytes.concat(hash1.data,hash2.data)
      return new ByteArrayWrapper(hasher.hash(joined))
    }


    val towerSer = new TowerSerializer(keySize=keySize,  hashSize = hasher.DigestSize)
    var rightRecids = mutable.ArrayBuffer(0L) // this is used to store links to already saved towers. Size==Max Level
    var prevKey:K = null; //used for assertion


    def makeHashes(level:Int, key:K, towerRight:List[Long]): List[Hash] ={
      assert(towerRight.size==level+1)
      assert(level>=0)
      //calculate hashes
      val rightHashes = new Array[Hash](level+1)

      rightHashes(0) =
        if(towerRight(0)==0){
          //no direct link, key hash
          if(prevKey==null) null //TODO null or empty hash for right most tower?
          else hashKey(prevKey)
        }else{
          //direct link, chain hash
          store.get(rightRecids(0), towerSer).hashes.head
        }

      for(level2 <- 1 to level){
        val rightRecid = rightRecids(level2)
        rightHashes(level2) =
          if(rightRecid==0) null
          else store.get(rightRecid, towerSer).hashes(level2)
      }

      val hashes = new Array[Hash](level+1)
      var bottomHash = hashKey(key)
      for(level2 <- 0 to level){
        hashes(level2) =
          if(towerRight(level2)==0L) bottomHash //right link not present, repeat hash
          else hashNode(bottomHash, rightHashes(level2)) //combine bottomHash and rightHash
        bottomHash = hashes(level2)
      }
      return hashes.toList
    }

    for((key, value)<-source) {
      //assertions
      assert(prevKey==null || prevKey.compareTo(key)>0, "source not sorted in ascending order")
      prevKey = key

      val level = levelFromKey(key)     //new tower will have this number of right links

      //grow to the size of `level`
      while(level+1>=rightRecids.size){
        rightRecids+=0L
      }
      //cut first `level` element for tower
      val towerRight = rightRecids.take(level+1).toList
      assert(towerRight.size==level+1)

      val hashes = makeHashes(level=level, key=key, towerRight=towerRight)
      //construct tower
      val tower = new Tower(key=key, value=value,
        right = towerRight,
        hashes = hashes.toList
      )

      val recid = store.put(tower, towerSer)

      //fill with  links to this tower
      rightRecids(level) = recid
      (0 until level).foreach(rightRecids(_)=0)

      assert(rightRecids.size>=level)
    }

    val hashes = makeHashes(key = new ByteArrayWrapper(keySize), level = rightRecids.length-1, towerRight = rightRecids.toList )

    //construct head
    val head = new Tower(
      key=new ByteArrayWrapper(keySize),
      value=new ByteArrayWrapper(0),
      right = rightRecids.toList,
      hashes = hashes
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
                    protected[skiplist] val store:Store,
                    protected[skiplist] val headRecid:Recid,
                    protected[skiplist] val keySize:Int,
                    protected[skiplist] val hasher:CryptographicHash = AuthSkipList.defaultHasher
                  ) {


  protected[skiplist] val towerSerializer = new TowerSerializer(keySize, hasher.DigestSize)

  protected[skiplist] def loadTower(recid:Recid): Tower ={
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


  def put(key: K, value:V){
    //grow head, so it is at least the same height as new tower
    val level = levelFromKey(key)
    var head = loadHead()
    while(head.right.size<=level){
      head = head.copy(
        right = head.right :+ 0L,
        hashes = head.hashes :+ new ByteArrayWrapper(hasher.DigestSize)
      )
      store.update(headRecid, head, towerSerializer)
    }

    val path = findPath(key, leftLinks = true, exact=false)
    assert(path!=null)

    if(path.tower.key == key){
      //update single tower with new value
      val tower = path.tower.copy(value=value)
      store.update(path.recid, tower, towerSerializer)
      return
    }

    //construct new tower
    val rightTowers = path.verticalRightTowers
    //zero out blind links
    for(level <- 0 until rightTowers.size){
      val (recid, tower) = rightTowers(level)
      if(tower!=null && tower.right.size-1!=level)
        rightTowers(level) = ((0L, null))
    }
    //fill rightTowers with null to level
    while(rightTowers.size<=level){
      rightTowers += ((0L, null))
    }
    val rightRecids = rightTowers.map(_._1).take(level+1)
    val hashes = (0 until level+1).map(a=>new ByteArrayWrapper(hasher.DigestSize)).toList

    val tower = new Tower(
      key = key,
      value = value,
      right =  rightRecids.toList,
      hashes = hashes
    )

    val insertedTowerRecid = store.put(tower, towerSerializer)

    //zero out right links for this node
    val leftRecids = path.verticalRecids
    for(level2 <- 0 until level){
      val recid = leftRecids(level2)
      var tower = loadTower(recid)
      tower = tower.copy(right = tower.right.updated(level2, 0L))
      store.update(recid, tower, towerSerializer)
    }
    //set link to current  node
    val leftRecid =  leftRecids(level)
    var leftTower = loadTower(leftRecid)
    leftTower = leftTower.copy(right = leftTower.right.updated(level, insertedTowerRecid))
    store.update(leftRecid, leftTower, towerSerializer)
  }

  def remove(key:K) : V = {
    var path = findPath(key, leftLinks = true)
    if(path==null)
      return null

    val towerRecid = path.recid
    val ret = path.tower.value
    while(path!=null){
      assert(path.tower.key == key)
      //replace right link on left neighbour
      var left = loadTower(path.leftRecid) //can not use path.left, since it could be modified during this iteration
      assert{val leftRightLink =left.right(path.level); leftRightLink==0 || leftRightLink == path.recid }
      val leftRightLinks = left.right.updated(path.level, path.tower.right(path.level))
      left = left.copy(right=leftRightLinks)
      store.update(path.leftRecid, left, towerSerializer)

      //move to upper node in path
      path = if(path.comeFromLeft) null else path.prev
    }
    store.delete(towerRecid, towerSerializer)
    return ret;
  }

  def findPath(key:K, leftLinks:Boolean=false, exact:Boolean = true ): PathEntry ={
    var recid  = headRecid
    var node = loadHead()
    var level = node.right.size-1
    var comeFromLeft=false
    var entry:PathEntry = null
    var justDive = false;

    while(node!=null && level>=0) {
      //load node, might be null
      val rightRecid = node.right(level)
      val rightTower = loadTower(rightRecid)

      var left:Tower = null
      var leftRecid = 0L

      if(leftLinks && entry!=null && entry.prev!=null){
        if(comeFromLeft) {
          //node on left is previous path entry
          leftRecid = entry.recid
          left = entry.tower
        }else{
          //get previous path entry left node
          leftRecid = entry.leftRecid
          left = entry.leftTower
          //and follow links non this level, until we reach this entry
          while(left!=null && left.right(level)!=0){
            //follow right link until we reach an end
            leftRecid = left.right(level)
            left = loadTower(leftRecid)
          }
        }
      }

      entry = PathEntry(
        prev=entry,
        recid = recid,
        tower=node,
        comeFromLeft = comeFromLeft,
        level=level,
        rightTower = rightTower,
        leftTower = left,
        leftRecid = leftRecid
      )

      if(justDive){
        if(level==0)
          return entry;
        comeFromLeft=false
        level-=1
      }else {

        //try to progress left
        val compare = if (rightTower == null) -1 else key.compareTo(rightTower.key)
        if (compare >= 0) {
          //key on right is smaller or equal, move right
          node = rightTower
          recid = rightRecid
          comeFromLeft = true
          if(compare==0) {
            //found match on right, so just keep diving
            justDive = true;
          }
        } else {
          comeFromLeft = false
          //key on right is bigger or non-existent, progress down
          level -= 1
        }
      }
    }
    return if(exact) null else entry

  }


  def findRight(path:PathEntry): (Recid,Tower) = {

    var entry = path
    while(entry!=null){
      //try to find entry, whatever comes first
      for(level <-entry.level+1 until entry.tower.right.size){
        val recid = entry.tower.right(level)
        if(recid!=0) {
          //fond right node
          return (recid, loadTower(recid))
        }
      }
      //not found right neighbour for this tower, progress to upper tower
      entry = entry.prev //TODO entry.prev could be the same, perf optimize
    }
    //not found
    return (0, null)
  }

  /** traverses skiplist and prints it structure */
  def printStructure(out:PrintStream = System.out): Unit ={
    def printRecur(recid:Recid, prefix:String): Unit ={
      val tower = loadTower(recid)
      out.println(prefix+recid + " - " + tower.right.size + " - " + tower.right)
      for(recid<-tower.right.filter(_!=0)) {
        printRecur(recid, prefix=prefix+"  ")
      }
    }

    out.println("=== SkipList ===")
    printRecur(headRecid, "  ")
  }

  /** calculates average level for each key */
  protected[skiplist] def calculateAverageLevel(): Double ={
    var sum = 0L
    var count = 0L

    def recur(tower:Tower): Unit ={
      count+=1
      sum += tower.right.size-1
      for(recid<-tower.right.filter(_!=0)) {
        recur(loadTower(recid))
      }
    }
    recur(loadHead())

    return 1D * sum/count
  }

  protected[skiplist] def verifyStructure(): Unit ={
    val recids = mutable.HashSet[Long]()

    def recur(recid:Long, level:Int): Unit ={
      val notExisted = recids.add(recid)
      assert(notExisted, "cyclic reference")
      val tower = loadTower(recid)
      assert(level == -1 || tower.right.size==level+1, "wrong tower height")
      for((recid:Recid, level:Int)<-tower.right.zipWithIndex) {
        if(recid!=0)
          recur(recid, level)
      }
    }
    recur(headRecid, -1)
  }

  protected[skiplist] def hashKey(key:K): Hash ={
    return  new ByteArrayWrapper(hasher.hash(key.data))
  }

  protected[skiplist] def hashNode(hash1:Hash, hash2:Hash):Hash = {
    assert(hash1.size==hasher.DigestSize)
    assert(hash2.size==hasher.DigestSize)
    val joined = Bytes.concat(hash1.data,hash2.data)
    return new ByteArrayWrapper(hasher.hash(joined))
  }
}

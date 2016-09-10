package io.iohk.iodb.skiplist

import java.io.PrintStream

import io.iohk.iodb.ByteArrayWrapper
import org.mapdb._
import scorex.crypto.hash.CryptographicHash

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AuthSkipList {


  def createEmpty(store: Store, keySize: Int, hasher: CryptographicHash = defaultHasher): AuthSkipList = {
    implicit val hasher2 = hasher
    //insert empty head
    val ser = new TowerSerializer(keySize = keySize, hashSize = hasher.DigestSize)
    val hash = hashNode(hashEntry(negativeInfinity, new V(0)), hashEntry(positiveInfinity, new V(0)))
    val headTower = Tower(
      key = null,
      value = null,
      right = List(0L),
      hashes = List(hash)
    )
    val headRecid = store.put(headTower, ser)
    new AuthSkipList(store = store, headRecid = headRecid, keySize = keySize, hasher = hasher)
  }

  def createFrom(source: Iterable[(K, V)], store: Store, keySize: Int, hasher: CryptographicHash = defaultHasher): AuthSkipList = {
    implicit val hasher2 = hasher

    val towerSer = new TowerSerializer(keySize = keySize, hashSize = hasher.DigestSize)
    var prevKey: K = positiveInfinity
    var prevValue = new V(0)
    def makeHashes(level: Int, key: K, value: V, towerRight: List[Long]): List[Hash] = {
      assert(towerRight.size == level + 1)
      assert(level >= 0)
      //calculate hashes
      val hashes = new Array[Hash](level + 1)
      var bottomHash = hashEntry(key, value)
      for (level2 <- 0 to level) {
        val rightRecid = towerRight(level2)
        val rightHash: Hash =
          if (level2 == 0 && rightRecid == 0) {
            //ground level, with no right link, use rightKey
            hashEntry(prevKey, prevValue)
          } else if (rightRecid == 0) {
            //no right links, reuse hash
            null
          } else {
            //load rightTower and use its hash at top level
            val rightTower = store.get(rightRecid, towerSer)
            assert(rightTower.right.size == level2 + 1)
            rightTower.hashes.last
          }

        bottomHash =
          if (rightHash == null) bottomHash
          else hashNode(bottomHash, rightHash)
        hashes(level2) = bottomHash
      }
      hashes.toList
    }


    var rightRecids = mutable.ArrayBuffer(0L) // this is used to store links to already saved towers. Size==Max Level

    for ((key, value) <- source) {
      //assertions
      assert(prevKey == null || prevKey.compareTo(key) > 0, "source not sorted in ascending order")

      val level = levelFromKey(key) //new tower will have this number of right links
      //grow to the size of `level`
      while (level >= rightRecids.size) {
        rightRecids += 0L
      }
      //cut first `level` element for tower
      val towerRight = rightRecids.take(level + 1).toList
      assert(towerRight.size == level + 1)

      val hashes = makeHashes(level = level, key = key, value = value, towerRight = towerRight)
      //construct tower
      val tower = Tower(
        key = key,
        value = value,
        right = towerRight,
        hashes = hashes
      )

      val recid = store.put(tower, towerSer)

      //fill with  links to this tower
      rightRecids(level) = recid
      (0 until level).foreach(rightRecids(_) = 0)

      assert(rightRecids.size >= level)
      prevKey = key
      prevValue = value
    }

    val hashes = makeHashes(key = negativeInfinity, value = new V(0), level = rightRecids.length - 1, towerRight = rightRecids.toList)

    //construct head
    val head = Tower(
      key = null,
      value = null,
      right = rightRecids.toList,
      hashes = hashes
    )
    val headRecid = store.put(head, towerSer)
    new AuthSkipList(store = store, headRecid = headRecid, keySize = keySize, hasher = hasher)
  }

}


/**
  * Authenticated Skip List implemented on top of MapDB Store
  */
class AuthSkipList(
                    protected[skiplist] val store: Store,
                    val headRecid: Recid,
                    protected[skiplist] val keySize: Int,
                    implicit protected[skiplist] val hasher: CryptographicHash = defaultHasher
                  ) extends Iterable[(K, V)] {


  protected[skiplist] val towerSerializer = new TowerSerializer(keySize, hasher.DigestSize)

  protected[skiplist] def loadTower(recid: Recid): Tower = {
    if (recid == 0L) null
    else store.get(recid, towerSerializer)
  }

  protected[skiplist] def loadHead() = store.get(headRecid, towerSerializer)


  protected[skiplist] def loadHeadEnsureLevel(level: Int) = {
    var head = loadHead()
    while (head.right.size <= level) {
      head = head.copy(
        right = head.right :+ 0L,
        hashes = head.hashes :+ new ByteArrayWrapper(hasher.DigestSize)
      )
      store.update(headRecid, head, towerSerializer)
    }
    head
  }


  def rootHash() = loadHead().hashes.last

  def close() = store.close()

  def get(key: K): V = {
    var node = loadHead()
    var level = node.right.size - 1
    while (node != null && level >= 0) {
      //load node, might be null
      val rightTower = loadTower(node.right(level))
      //try to progress left
      val compare = if (rightTower == null) -1 else key.compareTo(rightTower.key)
      if (compare == 0) {
        //found match, return value from right node
        return rightTower.value
      } else if (compare > 0) {
        //key on right is smaller, move right
        node = rightTower
      } else {


        //key on right is bigger or non-existent, progress down
        level -= 1
      }
    }
    null
  }

  def getPath(key: V): SkipListPath = {
    var path = findPath(key, exact = true, leftLinks = false)
    if (path == null)
      return null

    val value = loadTower(path.recid).value
    val p = new ArrayBuffer[SkipListPathEntry]()

    var isRightHash = true

    //iterate over path, update hash on each node
    while (path != null) {
      var tower: Tower = loadTower(path.recid)
      val rightLink = tower.right(path.level)
      val rightHash =
        if (path.level == 0 && rightLink == 0L) {
          //ground level, with no right link, use right key
          val rightTower = loadTower(path.findRight())
          if (rightTower == null) {
            //left most, use positive infinity as a key for hash
            hashEntry(positiveInfinity, new V(0))
          } else {
            hashEntry(rightTower.key, rightTower.value)
          }
        } else if (rightLink == 0L) {
          //no right nodes, reuse bottom hash
          null
        } else {
          //take hash from right node
          val rightTower = loadTower(path.rightRecid)
          assert(path.level + 1 == rightTower.hashes.size)
          rightTower.hashes.last
        }

      val bottomHash =
        if (path.level == 0) {
          if (tower.key == null) {
            hashEntry(negativeInfinity, new V(0))
          } else {
            hashEntry(tower.key, tower.value)
          }
        } else {
          tower.hashes(path.level - 1)
        }

      p += new SkipListPathEntry(
        sideHash = if (isRightHash) rightHash else bottomHash,
        isRightHash = isRightHash
      )
      isRightHash = !path.comeFromLeft

      //move to previous entry on path
      path = path.prev
    }


    new SkipListPath(
      key = key,
      value = value,
      hashPath = p.toList,
      hasher = hasher)
  }


  def put(key: K, value: V) {
    //grow head, so it is at least the same height as new tower
    val level = levelFromKey(key)
    loadHeadEnsureLevel(level)

    val path = findPath(key, leftLinks = true, exact = false)
    assert(path != null)

    val baseTower = loadTower(path.recid)
    if (baseTower.key == key) {
      //update single tower with new value
      val newTower = baseTower.copy(value = value)
      store.update(path.recid, newTower, towerSerializer)
      rehash(key)
      rehash(lowerKey(key))
      return
    }

    //construct new tower
    val rightTowers = path.verticalRightTowers
    //zero out blind links
    for (level <- rightTowers.indices) {
      val recid = rightTowers(level)
      val tower = loadTower(recid)
      if (tower != null && tower.right.size - 1 != level)
        rightTowers(level) = 0L //todo: check
    }
    //fill rightTowers with null to level
    while (rightTowers.size <= level) {
      rightTowers += 0L
    }
    val rightRecids = rightTowers.take(level + 1)
    val hashes = (0 until level + 1).map(a => new ByteArrayWrapper(hasher.DigestSize)).toList

    val tower = Tower(
      key = key,
      value = value,
      right = rightRecids.toList,
      hashes = hashes
    )

    val insertedTowerRecid = store.put(tower, towerSerializer)

    //zero out right links for this node
    val leftRecids = path.verticalRecids
    for (level2 <- 0 until level) {
      val recid = leftRecids(level2)
      var tower = loadTower(recid)
      tower = tower.copy(right = tower.right.updated(level2, 0L))
      store.update(recid, tower, towerSerializer)
    }
    //set link to current  node
    val leftRecid = leftRecids(level)
    var leftTower = loadTower(leftRecid)
    leftTower = leftTower.copy(right = leftTower.right.updated(level, insertedTowerRecid))
    store.update(leftRecid, leftTower, towerSerializer)
    rehash(key)
    rehash(lowerKey(key))
  }

  def lowerKey(key: K): K = {
    var node = loadHead()
    var level = node.right.size - 1
    while (node != null && level >= 0) {
      //load node, might be null
      val rightTower = loadTower(node.right(level))
      //try to progress left
      val compare = if (rightTower == null) -1 else key.compareTo(rightTower.key)
      if (compare > 0) {
        //key on right is smaller, move right
        node = rightTower
      } else {
        //key on right is bigger or non-existent, progress down
        level -= 1
      }
    }
    node.key
  }

  protected[skiplist] def rehash(key: K) {
    var path = findPath(key, leftLinks = false, exact = key != null)
    assert(key == null || get(key) != null) //if key is null it is negativeInfinity, rehash the head

    //iterate over path, update hash on each node
    while (path != null) {
      var tower: Tower = loadTower(path.recid)
      val rightLink = tower.right(path.level)
      val rightHash =
        if (path.level == 0 && rightLink == 0L) {
          //ground level, with no right link, use right key
          val rightTower = loadTower(path.findRight())
          if (rightTower == null) {
            //left most, use positive infinity as a key for hash
            hashEntry(positiveInfinity, new V(0))
          } else {
            hashEntry(rightTower.key, rightTower.value)
          }
        } else if (rightLink == 0L) {
          //no right nodes, reuse bottom hash
          null
        } else {
          //take hash from right node
          val rightTower = loadTower(path.rightRecid)
          assert(path.level + 1 == rightTower.hashes.size)
          rightTower.hashes.last
        }

      val bottomHash =
        if (path.level == 0) {
          if (tower.key == null) {
            hashEntry(negativeInfinity, new V(0))
          } else {
            hashEntry(tower.key, tower.value)
          }
        } else {
          tower.hashes(path.level - 1)
        }

      //update node
      val newHash =
        if (rightHash == null) bottomHash
        else hashNode(bottomHash, rightHash)

      tower = tower.copy(hashes = tower.hashes.updated(path.level, newHash))
      store.update(path.recid, tower, towerSerializer)

      //move to previous entry on path
      path = path.prev
    }
  }

  def remove(key: K): V = {
    var path = findPath(key, leftLinks = true, exact = true)
    if (path == null)
      return null

    var origpath = path
    val baseRecid = path.recid
    val tower = loadTower(path.recid)
    assert(tower.key == key)
    val ret = tower.value
    while (path != null) {
      assert(tower.key == key)
      //replace right link on left neighbour
      var left = loadTower(path.leftRecid)
      assert {
        val leftRightLink = left.right(path.level)
        leftRightLink == 0L || leftRightLink == path.recid
      }
      val leftRightLinks = left.right.updated(path.level, tower.right(path.level))
      left = left.copy(right = leftRightLinks)
      store.update(path.leftRecid, left, towerSerializer)

      //move to upper node in path
      path = if (path.comeFromLeft) null else path.prev
    }
    store.delete(baseRecid, towerSerializer)

    //collapse head, until it has zero right links on top
    while (loadHead().right.size > 1 && loadHead().right.last == 0L) {
      var head = loadHead()
      head = head.copy(
        hashes = head.hashes.dropRight(1),
        right = head.right.dropRight(1)
      )
      store.update(headRecid, head, towerSerializer)
    }

    rehash(lowerKey(key))
    ret
  }

  def findPath(key: K, leftLinks: Boolean = false, exact: Boolean = true): PathEntry = {
    var recid = headRecid
    var node = loadHead()
    var level = node.right.size - 1
    var comeFromLeft = false
    var entry: PathEntry = null
    var justDive = false

    while (node != null && level >= 0) {
      //load node, might be null
      val rightRecid = node.right(level)
      val rightTower = loadTower(rightRecid)

      var leftRecid = 0L

      if (leftLinks && entry != null) {
        if (comeFromLeft) {
          //node on left is previous path entry
          leftRecid = entry.recid
        } else {
          //get previous path entry left node
          leftRecid = entry.leftRecid
          var left = loadTower(leftRecid)
          //and follow links non this level, until we reach this entry
          while (left != null && left.right(level) != 0L) {
            //follow right link until we reach an end
            leftRecid = left.right(level)
            left = loadTower(leftRecid)
          }
        }
      }

      entry = PathEntry(
        prev = entry,
        recid = recid,
        comeFromLeft = comeFromLeft,
        level = level,
        rightRecid = rightRecid,
        leftRecid = leftRecid
      )

      if (justDive) {
        if (level == 0)
          return entry
        comeFromLeft = false
        level -= 1
      } else {

        //try to progress right
        val compare =
          if (rightTower == null || key == null) -1
          else key.compareTo(rightTower.key)
        if (compare >= 0) {
          //key on right is smaller or equal, move right
          node = rightTower
          recid = rightRecid
          comeFromLeft = true
          if (compare == 0) {
            //found match on right, so just keep diving
            justDive = true
          }
        } else {
          comeFromLeft = false
          //key on right is bigger or non-existent, progress down
          level -= 1
        }
      }
    }
    if (exact) null else entry
  }


  def findRight(path: PathEntry): (Recid, Tower) = {

    var entry = path
    while (entry != null) {
      //try to find entry, whatever comes first
      val tower = loadTower(entry.recid)
      for (level <- entry.level + 1 until tower.right.size) {
        val recid = tower.right(level)
        if (recid != 0L) {
          //fond right node
          return (recid, loadTower(recid))
        }
      }
      //not found right neighbour for this tower, progress to upper tower
      entry = entry.prev //TODO entry.prev could be the same, perf optimize
    }
    //not found
    (0, null)
  }

  /** traverses skiplist and prints it structure */
  def printStructure(out: PrintStream = System.out): Unit = {
    def printRecur(recid: Recid, prefix: String): Unit = {
      val tower = loadTower(recid)
      out.println(prefix + recid + " - " + tower.right.size + " - " + tower.right)
      for (recid <- tower.right.filter(_ != 0)) {
        printRecur(recid, prefix = prefix + "  ")
      }
    }

    out.println("=== SkipList ===")
    printRecur(headRecid, "  ")
  }

  /** calculates average level for each key */
  protected[skiplist] def calculateAverageLevel(): Double = {
    var sum = 0L
    var count = 0L

    def recur(tower: Tower): Unit = {
      count += 1
      sum += tower.right.size - 1
      for (recid <- tower.right.filter(_ != 0)) {
        recur(loadTower(recid))
      }
    }
    recur(loadHead())

    1D * sum / count
  }

  protected[skiplist] def verifyStructure(): Unit = {
    val recids = mutable.HashSet[Long]()

    def recur(recid: Long, level: Int): Unit = {
      val notExisted = recids.add(recid)
      assert(notExisted, "cyclic reference")
      val tower = loadTower(recid)
      assert(level == -1 || tower.right.size == level + 1, "wrong tower height")
      for ((recid: Recid, level: Int) <- tower.right.zipWithIndex) {
        if (recid != 0L)
          recur(recid, level)
      }
    }
    recur(headRecid, -1)
  }

  /**
    * Calculates rootHash by traversing recursively entire tree,
    */
  protected[skiplist] def verifyHash(): Unit = {

    /** recursive function used to calculate hash */
    def hash(tower: Tower, parentRightLink: Recid, level: Int): Hash = {
      val rightLink = tower.right(level)

      val bottomHash: Hash =
        if (level == 0) {
          //bottom level, hash key
          if (tower.key == null) hashEntry(negativeInfinity, new V(0))
          else hashEntry(tower.key, tower.value)
        } else {
          //not bottom, progress in recursion
          hash(
            tower,
            parentRightLink =
              if (rightLink != 0L) rightLink
              else parentRightLink,
            level = level - 1
          )
        }
      val rightHash: Hash =
        if (rightLink == 0L) {
          //no right link,
          if (level > 0) {
            null // no ground link, reuse bottom hash
          } else {
            //at ground level, use right key hash
            if (parentRightLink == 0L) {
              //no right keys, so use positive infinity
              hashEntry(positiveInfinity, new V(0))
            } else {
              //load next tower to get key and value
              val rightTower = loadTower(parentRightLink)

              hashEntry(rightTower.key, rightTower.value)
            }
          }
        } else {
          //right link is set, so use recursion to calculate its hash
          hash(tower = loadTower(rightLink), parentRightLink = parentRightLink, level = level)
        }

      if (rightHash == null) bottomHash
      else hashNode(bottomHash, rightHash)
    }

    val rootHash = hash(tower = loadHead(), parentRightLink = 0L, level = loadHead().hashes.size - 1)
    assert(rootHash == loadHead().hashes.last)


  }

  override def iterator: Iterator[(K, V)] = {
    //load all entries to list
    val ret = new mutable.ArrayBuffer[(K, V)]

    def recur(recid: Recid): Unit = {
      val tower = loadTower(recid)
      if (tower.key != null)
        ret += ((tower.key, tower.value))
      for (recid <- tower.right.filter(_ != 0)) {
        recur(recid)
      }
    }
    recur(headRecid)
    return ret.iterator
  }
}
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
  type Hash = Array[Byte]

  def defaultHasher = Blake2b256;
  protected[iodb] val nullArray = new Array[Byte](0);

  def empty(store:Store, keySize:Int, hasher:CryptographicHash = defaultHasher):AuthSkipList = {
    val headRecid = store.put(null, new NodeSerializer(keySize=keySize, hasher.DigestSize ))
    new AuthSkipList(store=store, headRecid = headRecid, keySize=keySize, hasher=hasher)
  }


}
import AuthSkipList._

protected[iodb] case class Node(key:K, value:V=null, hash:Hash, bottomLink:Long, rightLink:Long){}

/**
  * Authenticated Skip List implemented on top of MapDB Store
  */
class AuthSkipList(
                    protected val store:Store,
                    protected val headRecid:Long,
                    protected val keySize:Int,
                    protected val hasher:CryptographicHash = AuthSkipList.defaultHasher
) {

  protected val nodeSerializer = new NodeSerializer(keySize, hasher.DigestSize)

  protected def loadNode(recid:Long): Node ={
    if(recid==0) null
    else store.get(recid, nodeSerializer)
  }
  protected[iodb] def loadHead() = store.get(headRecid, nodeSerializer)
  protected def loadRecidHead():(Recid, Node) = (headRecid, store.get(headRecid, nodeSerializer))



  /** calculates node hash from key, value and hashes of bottom and right nodes */
  def nodeHash(key:K, value:V, rightHash:Hash, bottomHash:Hash): Hash ={
    //concat all byte arrays
    val concat = Bytes.concat(
      key.data,
      if(value==null) nullArray else value.data,
      if(rightHash==null) nullArray else rightHash,
      if(bottomHash==null) nullArray else bottomHash
    )
    //and hash the result
    return hasher(concat);
  }

  def close() = store.close()

  def nodeHashFromRecid(recid: Recid):Hash = {
    if(recid==0) null
    else store.get(recid, nodeSerializer).hash
  }

  def put(key:K, value:V): Unit ={
    //TODO for now always insert to lower level
    val path = mutable.Buffer.empty[(Recid, Node)]
    var node = loadRecidHead()
    if(node._2==null){
      //empty root, insert first node
      val hash = nodeHash(key=key, value=value, bottomHash = null, rightHash = null)
      val newNode = Node(key=key, value=value, hash=hash, bottomLink = 0, rightLink = 0)
      store.update(headRecid, newNode, nodeSerializer)
      return
    }
    while(node!=null){
      path+=node
      node =  nextRecidNode(key, node._2)
    }

    var old = path.last
    //now `old` is lower or equal to key, insert into linked list
    //get hash of next node
    val rightNodeHash = nodeHashFromRecid(old._2.rightLink)
    //insert new node
    var hash = nodeHash(key=key, value=value, bottomHash = null, rightHash = rightNodeHash)

    //insert new node
    val newNode = Node(key=key, value=value, hash=hash, bottomLink = 0, rightLink = old._2.rightLink)
    //update old node to point into new node
    var newRecid = store.put(newNode, nodeSerializer)
    //update other nodes along path
    for(node <- path.reverseIterator){
      var n = node._2
      hash = nodeHash(key=n.key, value=n.value,  bottomHash=null, rightHash = hash)
      n = n.copy(hash=hash,
        rightLink = if(newRecid!=0)newRecid else n.rightLink)

      store.update(node._1, n, nodeSerializer)
      newRecid = 0 //update rightLink only on first node
    }
  }

  def get(key:K):V= {
    var node = loadHead()
    while(node!=null){
      if(node.value!=null
        && key.compareTo(node.key)==0) //TODO this comparision can be refactored away, we already followed this node in `nextNode()`
        return node.value
      node = nextNode(key, node)
    }
    //reached end, not found
    return null
  }

  /** Returns next node for given key.
    * It follows right link, if right node is greater or equal.
    * It follows bottom link otherwise.
    */
  protected def nextNode(key:K, node:Node): Node = {
    val rightNode = loadNode(node.rightLink)
    return if(rightNode!=null && key.compareTo(rightNode.key)>=0)
        rightNode
      else
        loadNode(node.bottomLink)
  }

  /** Returns next node for given key with recid.
    * It follows right link, if right node is greater or equal.
    * It follows bottom link otherwise.
    */
  protected def nextRecidNode(key:K, node:Node): (Recid,Node) = {
    val rightNode = loadNode(node.rightLink)
    val ret = if(rightNode!=null && key.compareTo(rightNode.key)>=0)
        (node.rightLink, rightNode)
      else
        (node.bottomLink,loadNode(node.bottomLink))
    return if(ret._2==null) null else ret
  }

  /** Level for each key is not determined by probability, but from key hash to make Skip List structure deterministic.
    * Probability is simulated by checking if hash is dividable by a number without remainder (N % probability == 0).
    * At each level divisor increases exponentially.
    */
  protected def levelFromKey(key:K):Int={
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

  def printStructure(out:PrintStream = System.out): Unit ={
    out.println("=== SkipList ===")
    var recid = headRecid
    while(recid!=0){
      val n = loadNode(recid)
      out.println(s"recid=${recid}, hash=${n.hash}, key=${n.key}")
      recid = n.rightLink
    }
    out.println("")
  }
}

protected[iodb] class NodeSerializer(val keySize:Int, val hashSize:Int) extends Serializer[Node]{

  override def serialize(out: DataOutput2, node: Node): Unit = {
    out.packLong(node.bottomLink)
    out.packLong(node.rightLink)
    out.write(node.hash)
    out.write(node.key.data)
    if(node.value==null){
      out.packInt(0)
      return
    }
    out.packInt(node.value.data.length)
    out.write(node.value.data)
  }

  override def deserialize(input: DataInput2, available: Int): Node = {
    val bottomLink = input.unpackLong()
    val rightLink = input.unpackLong()
    val hash = new Array[Byte](hashSize)
    input.readFully(hash)
    val key = new Array[Byte](keySize)
    input.readFully(key)
    //read value if t exists
    val valueSize = input.unpackInt()
    val value = if(valueSize==0) null else {
      val b = new Array[Byte](valueSize)
      input.readFully(b)
      new ByteArrayWrapper(b)
    }

    return new Node(
      key=new ByteArrayWrapper(key), value=value,
      hash=hash, bottomLink=bottomLink, rightLink=rightLink)
  }
}

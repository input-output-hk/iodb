package io.iohk.iodb.skiplist

import io.iohk.iodb.ByteArrayWrapper
import org.mapdb._


object AuthSkipList{
  type K = ByteArrayWrapper
  type V = ByteArrayWrapper
  type Recid = Long

  def empty(store:Store, keySize:Int):AuthSkipList = {
    //insert empty root node
    val lowestKey = new ByteArrayWrapper(new Array[Byte](keySize))
    val headNode = new Node(key = lowestKey, value=null, rightLink = 0L, bottomLink = 0L)
    val headRecid = store.put(headNode, new NodeSerializer(keySize=keySize))
    new AuthSkipList(store=store, headRecid = headRecid, keySize=keySize)
  }
}
import AuthSkipList._

protected[iodb] case class Node(key:K, value:V=null, bottomLink:Long, rightLink:Long){}

/**
  * Authenticated Skip List implemented on top of MapDB Store
  */
class AuthSkipList(
                    protected val store:Store,
                    protected val headRecid:Long,
                    protected val keySize:Int
) {

  protected val nodeSerializer = new NodeSerializer(keySize)

  protected def loadNode(recid:Long): Node ={
    if(recid==0) null
    else store.get(recid, nodeSerializer)
  }
  protected def loadHead() = store.get(headRecid, nodeSerializer)
  protected def loadRecidHead():(Recid, Node) = (headRecid, store.get(headRecid, nodeSerializer))


  def close() = store.close()

  def put(key:K, value:V): Unit ={
    //TODO for now always insert to lower level
    var node = loadRecidHead()
    var old = node;
    while(node!=null){
      old = node;
      node =  nextRecidNode(key, node._2)
    }

    //now `old` is lower or equal to key, insert into linked list
    //insert new node
    val newNode = Node(key=key, value=value, bottomLink = 0, rightLink = old._2.rightLink)
    val newRecid = store.put(newNode, nodeSerializer)
    //update old node to point into new node
    val oldNode2 = old._2.copy(rightLink = newRecid)
    store.update(old._1, oldNode2, nodeSerializer)
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
    * It follows right link, if right node has lower or equal key.
    * It follows bottom link otherwise.
    */
  protected def nextNode(key:K, node:Node): Node = {
    val rightNode = loadNode(node.rightLink)
    return if(rightNode!=null && key.compareTo(rightNode.key)<=0)
        rightNode
      else
        loadNode(node.bottomLink)
  }

  /** Returns next node for given key with recid.
    * It follows right link, if right node has lower or equal key.
    * It follows bottom link otherwise.
    */
  protected def nextRecidNode(key:K, node:Node): (Recid,Node) = {
    val rightNode = loadNode(node.rightLink)
    val ret = if(rightNode!=null && key.compareTo(rightNode.key)<=0)
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


}


protected[iodb] class NodeSerializer(val keySize:Int) extends Serializer[Node]{

  override def serialize(out: DataOutput2, node: Node): Unit = {
    out.packLong(node.bottomLink)
    out.packLong(node.rightLink)
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
      bottomLink=bottomLink, rightLink=rightLink)
  }
}

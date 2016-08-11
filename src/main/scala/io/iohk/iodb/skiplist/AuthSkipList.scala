package io.iohk.iodb.skiplist

import io.iohk.iodb.ByteArrayWrapper
import org.mapdb._

/**
  * Authenticated Skip List implemented on top of MapDB Store
  */
class AuthSkipList(
    protected val store:Store,
    protected val rootRecid:Long,
    protected val keySize:Int
) {

  type K = ByteArrayWrapper
  type V = ByteArrayWrapper

  protected def loadNode(recid:Long): Node ={
    if(recid==0) null
    else store.get(recid, NodeSerializer)
  }
  protected def loadHead() = store.get(rootRecid, NodeSerializer)

  def close() = store.close()

  def put(key:K, value:V): Unit ={

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

  /** returns next node for given key.
    * It goes right, if right node has lower or equal key.
    * It goes down otherwise.
    * */
  protected def nextNode(key:K, node:Node): Node = {
    val rightNode = loadNode(node.rightLink)
    return if(rightNode!=null && key.compareTo(rightNode.key)<=0)
        rightNode
      else
        loadNode(node.bottomLink)
  }


  /** Level for each key is not determined by probability, but from key hash to make Skip List structure deterministic.
    * Probability is simulated by checking if hash is dividable by a number with no remainder (N % probability == 0).
    * At each level increases divisor increases exponentially.
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

  //TODO make this static class
  protected case class Node(key:K, value:V=null, bottomLink:Long, rightLink:Long){}

  protected object NodeSerializer extends Serializer[Node]{

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
}


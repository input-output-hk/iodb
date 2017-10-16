package io.iohk.iodb

import io.iohk.iodb.Store._


object ShardedIterator {

  /** iterator that merges key-value iterators from Journal and Shards */
  def distIter(
                journalIter: Iterator[(K, V)],
                shardIters: Seq[Iterator[(K, V)]]
              ): Iterator[(K, V)] = {

    if (journalIter.isEmpty && shardIters.forall(_.isEmpty))
      return None.iterator


    if (shardIters.isEmpty)
      return journalIter.filterNot(_._2 eq tombstone)

    object o extends Iterator[(K, V)] {

      val shards = shardIters.iterator
      var shard = shards.next()

      var j = journalNext()
      var s = shardNext()

      var next2: Option[(K, V)] = null

      def journalNext(): (K, V) = {
        if (journalIter.hasNext) journalIter.next()
        else null
      }

      def shardNext(): (K, V) = {
        while (!shard.hasNext) {
          //advance to shard with data
          if (shards.isEmpty) {
            return null
          }
          shard = shards.next()
        }

        return shard.next()
      }


      def advance(): Option[(K, V)] = {
        //FIXME this method is called recursively, could cause StackOverflow with too many tombstones, tailrec fails

        if (j == null && s == null) {
          //both empty
          return None
        } else if (j == null && s != null) {
          val s2 = s
          s = shardNext()
          if (s2._2 eq tombstone)
            return advance()
          return Some(s2)
        } else if (j != null && s == null) {
          val j2 = j
          j = journalNext()
          if (j2._2 eq tombstone)
            return advance()
          return Some(j2)
        } else if (s._1 == j._1) {
          //both are at the same key, take newer value from journal, advance both
          val j2 = j
          j = journalNext()
          s = shardNext()
          if (j2._2 eq tombstone)
            return advance()
          return Some(j2)
        } else if (j._1 < s._1) {
          //take from journal, advance journal
          val j2 = j
          j = journalNext()
          if (j2._2 eq tombstone)
            return advance()
          return Some(j2)
        } else if (j._1 > s._1) {
          //take from shard, advance shard
          val s2 = s
          s = shardNext()
          if (s2._2 eq tombstone)
            return advance()
          return Some(s2)
        } else {
          throw new IllegalStateException()
        }

      }

      override def hasNext: Boolean = {
        if (next2 == null)
          next2 = advance()

        return next2.isDefined
      }

      override def next(): (K, V) = {
        if (next2 == null)
          next2 = advance()

        if (next2.isEmpty)
          throw new NoSuchElementException()

        val next3 = next2
        next2 = null
        return next3.get
      }
    }

    return o


  }

}
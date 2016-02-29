package com.alpine.util

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
case class FilteredSeq[A](originalSeq: Seq[A], indicesToUse: Seq[Int]) extends Seq[A] {
  override def length: Int = indicesToUse.length

  override def apply(idx: Int): A = originalSeq(indicesToUse(idx))

  override def iterator: Iterator[A] = {
    new Iterator[A] {
      val indexIterator = indicesToUse.iterator

      override def hasNext: Boolean = indexIterator.hasNext

      override def next(): A = originalSeq(indexIterator.next())
    }
  }
}

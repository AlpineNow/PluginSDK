package com.alpine.model.pack.util

/**
  * Utility functions that Transformers may need.
  */
object TransformerUtil {

  /**
    * WARNING: Mutates input.
    *
    * Puts the contents of row into the tempArray argument.
    * The two arguments must be the same length.
    * That is, we require {{{row.length == tempArray.length}}}
    *
    * Uses [[anyToDouble]] for double conversion.
    *
    * @param row       contains the values to be read.
    * @param tempArray the array to be written to.
    * @return reference to tempArray, filled with the contents of the row.
    */
  def fillRowToDoubleArray(row: Seq[Any], tempArray: Array[Double]): Array[Double] = {
    var i = 0
    while (i < tempArray.length) {
      tempArray(i) = anyToDouble(row(i))
      i += 1
    }
    tempArray
  }

  def normalizeProbabilities(array: Seq[Double]): Seq[Double] = {
    val sum = array.sum
    if (sum != 0) {
      array.map(d => d / sum)
    } else {
      array
    }
  }

  /**
    * Converts input of type Any to Double.
    * Does this by casting to java.lang.Number,
    * and then taking the double value.
    *
    * Will return Double.NaN in the case of bad input.
    *
    * @param a input to be converted to Double.
    * @return Double representation of the number, or Double.NaN if impossible.
    */
  def anyToDouble(a: Any): Double = {
    try {
      a.asInstanceOf[Number].doubleValue()
    }
    catch {
      case _: NullPointerException => Double.NaN
      case _: ClassCastException => Double.NaN
    }
  }

  def toJavaDoubleSeq(doubles: Seq[Double]): Seq[java.lang.Double] = {
    doubles.map(d => d.asInstanceOf[java.lang.Double])
  }

  def javaDoubleSeqToArray(doubles: Seq[java.lang.Double]): Array[Double] = {
    doubles.map(d => d.doubleValue()).toArray
  }
}

/**
  * Dresses the row argument in a Seq[Double].
  * Elements that can't be casted to java.lang.Number will
  * be represented by Double.NaN.
  *
  * @param row Seq[Any] to be dressed as Seq[Double]
  */
case class CastedDoubleSeq(row: Seq[Any]) extends Seq[Double] {

  override def length: Int = row.length

  override def iterator: Iterator[Double] = {
    new Iterator[Double] {
      private val rowIterator = row.iterator

      override def hasNext: Boolean = rowIterator.hasNext

      override def next(): Double = TransformerUtil.anyToDouble(rowIterator.next())
    }
  }

  override def apply(idx: Int): Double = TransformerUtil.anyToDouble(row(idx))

}
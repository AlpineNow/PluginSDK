/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.util

/**
  * Created by Jennifer Thompson on 1/23/17.
  */
object ModelUtil {

  /**
    * Creates a new Seq calculating the cumulative sum values from left to right.
    *
    * The returned Sequence will be one larger than the input Sequence.
    * The first value in the returned sequence will be 0,
    * and the last one will be the sum of the whole input sequence.
    *
    * @param values Seq of which to calculate the cumulative sum.
    * @return Seq of size one larger than the original, containing the cumulative sum values.
    */
  def cumulativeSum(values: Seq[Int]): Seq[Int] = {
    values.foldLeft(List[Int](0))((sumSoFar, value) => {
      (value + sumSoFar.head) :: sumSoFar
    }).reverse
  }

}

package utils

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/18/14
 * Time: 9:26 PM
 */

object Shuffle {

  /**
   * Fisher-Yates shuffle, also known as Algorithm P
   *
   * @param coll  input collection
   * @tparam T    element type
   * @return      the collection shuffled
   */
  def apply[T](coll: Array[T]) = {
    val len = coll.length
    var idx = 0
    while (idx < len - 1) {
      val rd = idx + Random.nextInt(len - idx)
      val elemOnIndex = coll(idx)
      coll(idx) = coll(rd)
      coll(rd) = elemOnIndex
      idx += 1
    }
    coll
  }

  /**
   * Fisher-Yates shuffle which takes only some elements
   *
   * @param coll  input collection
   * @param nb    take first nb shuffled elements
   * @tparam T    element type
   * @return
   */
  def apply[T](coll: Array[T], nb: Int) = {
    require(nb > 0)
    val len = coll.length
    val threshold = math.min(nb - 1, len - 1)
    var idx = 0
    while (idx < threshold) {
      val rd = idx + Random.nextInt(len - idx)
      val elemOnIndex = coll(idx)
      coll(idx) = coll(rd)
      coll(rd) = elemOnIndex
      idx += 1
    }
    coll.take(threshold + 1)
  }
}

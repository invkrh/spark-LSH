package com.github.invkrh.utils

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/18/14
 * Time: 9:51 PM
 */

import org.scalatest._

class ShuffleTest extends FlatSpec with Matchers {

  "Shuffle" should "give a randomized input array " in {
    val arr = Array(1, 2, 3, 4, 5, 6)
    val shuffled = Shuffle(arr)
    // shuffled foreach println
    shuffled.size shouldBe arr.size
  }

  "Shuffle" should "give a randomized input array of specified length" in {
    val arr = Array(1, 2, 3, 4, 5, 6)
    val shuffled = Shuffle(arr, 3)
    // shuffled foreach println
    shuffled.size shouldBe 3
  }

}

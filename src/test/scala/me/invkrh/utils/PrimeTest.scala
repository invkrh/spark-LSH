package com.github.invkrh.utils

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 12/9/14
 * Time: 10:57 PM
 */

class PrimeTest extends FlatSpec with Matchers {

  "nonCoPrimeNumbers" should "give a valid prime list" in {
    Prime.nonCoPrimeNumbers(12).take(10).toList shouldBe List(5, 7, 11, 13, 17, 19, 23, 29, 31, 37)
  }

}

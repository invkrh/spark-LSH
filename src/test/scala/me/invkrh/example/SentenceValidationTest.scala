package com.github.invkrh.example

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:50 PM
 */

import org.scalatest._
import com.github.invkrh.example.SentenceValidation._

class SentenceValidationTest extends FlatSpec with Matchers {

  "isEditDistance1" should "identify two sentences whose edit distance is 1" in {
    val ss = Array(
      (1, Array("A", "B", "C", "D")),
      (2, Array("A", "B", "X", "D")),
      (3, Array("A", "B", "C")),
      (4, Array("A", "B", "X", "C")))

    val validPair = for {
      i <- ss
      j <- ss if i._1 < j._1 && isEditDistance1(i._2, j._2)
    } yield (i._1, j._1)

    println(validPair.toList)
    validPair.size shouldBe 4
  }

}

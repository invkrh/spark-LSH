package example

import org.apache.commons.math3.primes.Primes

//import org.apache.commons.math3.primes.Primes

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 5:48 PM
 */

object SimilarSentences extends App {
  /**
   * 9,397,023 sentences
   * 1,350,205 distinct words
   * 10,392,158 distinct (word, index) pair
   * longest sentence: 12452 words
   * 1092 groups by size
   * 117 byte per sentence
   * 30 Integers in size per sentence
   */

  import setting.context._

  //  val sentences1 = sc.textFile("data/sentences.txt", 20).map {
  //    str => str.split(" ") match {
  //      case Array(id, s@_*) => (id.toInt, s.toArray)
  //    }
  //  } //.filter(_._1 < 100000)
  //
  val sentences2 = sc.parallelize(Array(
    (1, Array("A", "B", "C", "D")),
    (2, Array("A", "B", "X", "D")),
    (3, Array("A", "B", "C")),
    (4, Array("A", "B", "X", "C"))
  ))

  // Test validation function

  //  val s1 = Array("A", "B", "C", "D")
  //  val s2 = Array("A", "B", "X", "D")
  //  val s3 = Array("A", "B", "C")
  //  val s4 = Array("A", "B", "X", "C")
  //
  //  println(isValidate(s1, s2))
  //  println(isValidate(s1, s3))
  //  println(isValidate(s1, s4))
  //  println(isValidate(s2, s3))
  //  println(isValidate(s2, s4))
  //  println(isValidate(s3, s4))


  // data choice
  //  val sentences = sentences2

  import org.apache.spark.SparkContext._

}

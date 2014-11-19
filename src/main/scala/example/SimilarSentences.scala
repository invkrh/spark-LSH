package example

import core.{LSH, IndexedItemSet}

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

  val sentences2 = sc.parallelize(Array(
    (1, Array("A", "B", "C", "D")),
    (2, Array("A", "B", "X", "D")),
    (3, Array("A", "B", "C")),
    (4, Array("A", "B", "X", "C"))
  ))

  val sentences3 = sc.parallelize(Array(
    (1, Array("A", "C", "D")),
    (2, Array("B", "C", "E"))
  ))

  // data choice
  val sentences = sentences2

  val dataSet = sentences.map {
    case (id, words) => IndexedItemSet(id, words zipWithIndex)
  }

  val res = new LSH()
    .setBands(20)
    .setRows(5)
    .run(dataSet)

  res foreach println
}

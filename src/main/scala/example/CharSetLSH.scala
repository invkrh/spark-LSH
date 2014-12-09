package example

import core.{LSH, IndexedSet}

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 5:48 PM
 */

object CharSetLSH extends App {
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

  val charSet = sc.parallelize(Array(
    (1, Array("A", "B", "C", "D")),
    (2, Array("A", "B", "X", "D")),
    (3, Array("N", "D", "F")),
    (4, Array("Y", "E", "G", "V")),
    (5, Array("Z", "X", "V")),
    (6, Array("V", "X", "Z")),
    (7, Array("A", "B", "C", "D")),
    (8, Array("A", "B", "X", "D"))
  ))

  // data choice

  val dataSet = charSet.map {
    case (id, words) => IndexedSet(id, words.toSet)
  }

  val res = new LSH()
    .setBands(20)
    .setRows(5)
    .run(dataSet)

  res.collect foreach println
}

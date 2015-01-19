package com.github.invkrh.core

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:17 PM
 */

case class IndexedSet[T](index: Int, elems: Set[T])

class LSH private
(private var rows: Int,
 private var bands: Int) extends Serializable {

  def this() = this(5, 20)

  def setRows(numRows: Int): this.type = {
    this.rows = numRows
    this
  }

  def setBands(numBands: Int): this.type = {
    this.bands = numBands
    this
  }

  def generateMinHashFunctions(universeSize: Int): Array[MinHashFunction] = {

    /**
     * create a valid prime list by excluding prime factors of universeSize
     * make sure that each minhash function has a good distribution
     */

    import org.apache.commons.math3.primes.Primes
    val primeNumber = Primes.nextPrime(universeSize)

    Array.tabulate(bands * rows) {
      _ =>
        MinHashFunction(
          a = Random.nextInt(primeNumber),
          b = Random.nextInt(universeSize),
          primeNumber)
    }
  }

  def generateBandHashFunctions() =
    for {
      i <- 0 until bands
    } yield BandHashFunction(Random.nextInt)

  def run[T: ClassTag](itemSets: RDD[IndexedSet[T]]) = {

    val sparkContext = itemSets.context

    val dictionary = itemSets.map(_.elems)
      .flatMap(x => x)
      .distinct
      //.sortBy(identity) // Sort may be skipped for performance issue
      .collect

    println("dictionary.size = " + dictionary.size)

    val bdcUniversalSet = sparkContext.broadcast(dictionary)
    val bdcMinHashFunctions = sparkContext.broadcast(generateMinHashFunctions(dictionary.size))
    val bdcBandHashFunctions = sparkContext.broadcast(generateBandHashFunctions)

    itemSets.flatMap {
      case IndexedSet(id, set) =>

        val universalSet = bdcUniversalSet.value
        val minHashFuncs = bdcMinHashFunctions.value
        val bandHashFuncs = bdcBandHashFunctions.value

        val elemIndexInUniversalSet = set.map(p => universalSet.indexOf(p))

        val signatures = for {
          hash <- minHashFuncs
        } yield (elemIndexInUniversalSet map hash.apply).min

        signatures.sliding(rows, rows).zipWithIndex
          .map {
          case (arr, bandId) =>
            val bucketId = bandHashFuncs(bandId)(arr)
            (bandId, bucketId, id)
        }
    }.groupBy {
      case (bandId, bucketId, _) => (bandId, bucketId)
    }.values.map(iter => iter.map(_._3).toList).filter(_.size > 1).distinct
  }
}

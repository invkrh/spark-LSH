package core

import org.apache.commons.math3.primes.Primes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import utils.Shuffle

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:17 PM
 */


trait HashFunction

case class MinHashFunction(a: Int, b: Int, uz: Int) extends HashFunction {
  def apply(x: Int) = (a * x + b) % uz
}

case class BandHashFunction(seed: Int) extends HashFunction {
  def apply(x: Array[Int]) = scala.util.hashing.MurmurHash3.arrayHash(x, seed)
}

case class IndexedItemSet[T](id: Int, elems: Array[T])

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

  def minHashFunctions(universeSize: Int, number: Int): Array[MinHashFunction] = {
    val pFactors = Primes.primeFactors(universeSize)
    val validFactors = for {
      i <- 1 to universeSize + Random.nextInt(universeSize) if Primes.isPrime(i) && !pFactors.contains(i)
    } yield i
    Array.tabulate(number) {
      i =>
        MinHashFunction(validFactors(Random nextInt validFactors.size), Random.nextInt(universeSize), universeSize)
    }
  }

  def bandHashFunctions() =
    for {
      i <- 0 until bands
    } yield BandHashFunction(Random.nextInt)

  def run[T: ClassTag](itemSets: RDD[IndexedItemSet[T]]) = {

    val sparkContext = itemSets.context

    val dictionary = itemSets.map(_.elems)
      .flatMap(x => x)
      .distinct
      //.sortBy(identity) // Sort may be skipped for performance issue
      .collect

    val bdcDict = sparkContext.broadcast(dictionary)
    val bdcMinHashFunctions = sparkContext.broadcast(minHashFunctions(dictionary.size, bands * rows))
    val bdcBandHashFunctions = sparkContext.broadcast(bandHashFunctions)

    itemSets.flatMap {
      case IndexedItemSet(itemId, elems) =>
        val universalWords = bdcDict.value
        val minHash = bdcMinHashFunctions.value
        val bandFunction = bdcBandHashFunctions.value
        val rowsWithOne = elems.map(p => universalWords.indexOf(p))
        val signatures = for {
          hash <- minHash
        } yield (rowsWithOne map hash.apply).min

        signatures.sliding(rows, rows).zipWithIndex
          .map {
          case (arr, bandId) =>
            val bucketId = bandFunction(bandId)(arr)
            (bandId, bucketId, itemId)
        }
    }.groupBy {
      case (bandId, bucketId, _) => (bandId, bucketId)
    }.values.map(iter => iter.map(_._3).toList).filter(_.size > 1).distinct
  }
}
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

case class IndexedItemSet[T](id: Int, elems: Array[T])

class LSH private(private var rows: Int, private var bands: Int) {

  def this() = this(5, 20)

  def setRows(numRows: Int): this.type = {
    this.rows = numRows
    this
  }

  def setBands(numBands: Int): this.type = {
    this.bands = numBands
    this
  }


  def hashFunctionFamily(a: Int, b: Int, uz: Int): Int => Int = {
    x: Int => (a * x + b) % uz
  }

  def generateMinHashFunctions(universeSize: Int, number: Int): Array[Int => Int] = {
    val pFactors = Primes.primeFactors(universeSize)
    val validFactors = for {
      i <- 1 to universeSize + Random.nextInt(universeSize) if Primes.isPrime(i) && !pFactors.contains(i)
    } yield i
    Array.tabulate(number) {
      i =>
        hashFunctionFamily(validFactors(Random nextInt validFactors.size), Random.nextInt(universeSize), universeSize)
    }
  }

  def generateBandFunction(funcs: Array[Int => Int]) = {
    for {
      i <- 0 until bands
    } yield (xs: Array[Int]) => (xs zip Shuffle(funcs, rows)).map { case (x, f) => f(x)}.sum
  }

  def run[T: ClassTag](itemSets: RDD[IndexedItemSet[T]]): RDD[Array[Int]] = {

    val sparkContext = itemSets.context
    val numMinHashFunc = bands * rows

    val dictionary = itemSets.map(_.elems)
      .flatMap(x => x)
      .distinct
      //.sortBy(identity) // Sort may be skipped for performance issue
      .collect

    val minHashFunctions = generateMinHashFunctions(dictionary.size, numMinHashFunc)
    val bandHashFunctions = generateBandFunction(minHashFunctions)

    val bdcDict = sparkContext.broadcast(dictionary)
    val bdcMinHashFunctions = sparkContext.broadcast(minHashFunctions)
    val bdcBandHashFunctions = sparkContext.broadcast(bandHashFunctions)

    val retVal = itemSets.flatMap {
      case IndexedItemSet(itemId, elems) =>
        val universalWords = bdcDict.value
        val minHash = bdcMinHashFunctions.value
        val rowsWithOne = elems.map(p => universalWords.indexOf(p))
        val signatures = for {
          hash <- minHash
        } yield (rowsWithOne map hash).min
        val bandFunction = bdcBandHashFunctions.value
        (signatures.sliding(rows, rows).toArray zipWithIndex).map {
          case (arr, bandId) =>
            val bucketId = bandFunction(bandId)(arr)
            (bandId, bucketId, itemId)
        }
    }.groupBy {
      case (bandId, bucketId, _) => (bandId, bucketId)
    }.values.map(iter => iter.map(_._3).toArray)

    retVal
  }
}
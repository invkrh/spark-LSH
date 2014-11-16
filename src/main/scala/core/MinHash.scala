package core

import org.apache.commons.math3.primes.Primes
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:16 PM
 */

object MinHash {
  type HashFunction = Int => Int

  def hashFunctionFamily(a: Int, b: Int, uz: Int): HashFunction = {
    x: Int => (a * x + b) % uz
  }

  def generateHashFunctions(universeSize: Int, number: Int): Array[HashFunction] = {

    val pFactors = Primes.primeFactors(universeSize)
    val validFactors =
      for {
        i <- 1 to universeSize + Random.nextInt(universeSize) if Primes.isPrime(i) && !pFactors.contains(i)
      } yield i

    Array.tabulate(number) {
      i =>
        hashFunctionFamily(validFactors(Random nextInt validFactors.size), Random.nextInt(universeSize), universeSize)
    }
  }

  def run(mtx: RDD[Array[String]], hashFuncs: Array[HashFunction]) = {


  }


}

package core

import org.apache.commons.math3.primes.Primes

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:16 PM
 */

object MinHash {
  type HashFunction = Int => Int

  def generateHashFunctions(universeSize: Int, number: Int): Array[HashFunction] = {

    Array.tabulate(number) {
      i =>
        val rd = Random.nextInt(number * number)
        hashFunction(Primes.nextPrime(rd), Random.nextInt(universeSize), universeSize)
    }
  }

  def hashFunction(a: Int, b: Int, uz: Int): HashFunction = {
    x: Int => (a * x + b) % uz
  }

}

package core

import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 12/7/14
 * Time: 10:31 PM
 */

class MinhashTest extends FlatSpec with Matchers {
  "Prime.primeFactors" should "do the prime factore decomposition" in {
    import org.apache.commons.math3.primes.Primes
    val res = Primes.primeFactors(50)
    println(res)
  }

  "co-prime numbers" should "give a better hash distribution" in {
    /**
     * a and mod should be co-prime number
     */
    val a = 9
    val mod = 16
    val b = Random.nextInt(mod)
    def func(x: Int) = {
      (a * x + b) % mod
    }

    val dist = for {
      i <- 0 until mod
    } yield func(i)

    dist foreach println

    dist.distinct.size shouldBe mod
  }
}

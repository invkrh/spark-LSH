package com.github.invkrh.utils

import org.apache.commons.math3.primes.Primes

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 12/9/14
 * Time: 10:43 PM
 */

object Prime {

  def from(n: Int): Stream[Int] = n #:: from(n + 1)

  def sieve(s: Stream[Int]): Stream[Int] = s.head #:: sieve(s.tail filter (_ % s.head != 0))

  def nonCoPrimeNumbers(n: Int): Stream[Int] = {
    val primeFactors = Primes.primeFactors(n)
    sieve(from(2)).filter(x => !primeFactors.contains(x))
  }

}

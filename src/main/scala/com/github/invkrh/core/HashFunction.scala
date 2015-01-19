package com.github.invkrh.core

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 12/9/14
 * Time: 2:04 AM
 */

trait HashFunction

case class MinHashFunction(a: Int, b: Int, n: Int) extends HashFunction {
  def apply(x: Int) = (a * x + b) % n
}

case class BandHashFunction(seed: Int) extends HashFunction {
  def apply(x: Array[Int]) = scala.util.hashing.MurmurHash3.arrayHash(x, seed)
}

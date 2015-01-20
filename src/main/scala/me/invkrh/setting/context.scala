package com.github.invkrh.setting

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * User: invkrh
 * Date: 11/16/14
 * Time: 6:20 PM
 */

object context {
  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Finding_Similar_Sentence")
      .setMaster("local[8]")
      .set("spark.local.dir", "/tmp"))
}

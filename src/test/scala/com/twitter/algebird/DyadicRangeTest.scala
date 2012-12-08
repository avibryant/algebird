package com.twitter.algebird

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object DyadicRangeTest extends Properties("DyadicRange") {
  def checkRangeIndices(fn : (List[(Int,Long)] => Boolean)) = forAll {(a: Long, b: Long) =>
    if(a >= 0 && b > a) {
      fn(DyadicRange().indicesForRange(a,b))
    }
  }

  property("coverage") = checkRangeIndices { indices =>
    true   
  }
}
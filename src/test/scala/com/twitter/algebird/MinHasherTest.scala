package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

import java.util.Arrays

object MinHasherTest extends Properties("MinHasher") with BaseProperties {
  implicit val mhMonoid = new MinHasher32(0.5, 512)
  implicit val mhGen = Arbitrary { for(
      v <- choose(0,10000)
    ) yield (mhMonoid.init(v))
  }

  property("MinHasher is a Monoid") = monoidLawsEq[Array[Byte]]{(a,b) => a.toList == b.toList}
}

class MinHasherTest extends Specification {
  val r = new java.util.Random

  def testSimilarity[H](mh : MinHasher[H], similarity : Double, epsilon : Double) = {
    val (set1, set2) = randomSets(similarity)

    val exact = exactSimilarity(set1, set2)
    val sim = approxSimilarity(mh, set1, set2)
    val error = math.abs(exact - sim)
    println(exact, sim, error)
    error must be_<[Double](epsilon)
  }

  def testBuckets[H](mh : MinHasher[H]) = {
    var correctPositives = 0
    var correctNegatives = 0
    var falsePositives = 0
    var falseNegatives = 0
    val threshold = mh.estimatedThreshold
    println(threshold)
    1.to(10000).foreach{i => 
      val sim = math.random
      val matches = bucketsMatch(mh, sim)
      if(sim >= threshold) {
        if(matches)
          correctPositives += 1
        else
          falsePositives += 1
      } else {
        if(matches)
          falseNegatives += 1
        else
          correctNegatives += 1
      }
    }
    println((correctPositives, correctNegatives, falsePositives, falseNegatives))
  }

  def bucketsMatch[H](mh : MinHasher[H], similarity : Double) = {
    val (set1, set2) = randomSets(similarity)
    val buckets1 = mh.buckets(sig(mh, set1))
    val buckets2 = mh.buckets(sig(mh, set2))
    buckets1.exists{buckets2.contains(_)}
  }

  def randomSets(similarity : Double) = {
    val s = 100
    val uniqueFraction = if(similarity == 1.0) 0.0 else (1 - similarity)/(1 + similarity)
    val sharedFraction = 1 - uniqueFraction
    val unique1 = 1.to((s*uniqueFraction).toInt).map{i => math.random}.toSet
    val unique2 = 1.to((s*uniqueFraction).toInt).map{i => math.random}.toSet

    val shared = 1.to((s*sharedFraction).toInt).map{i => math.random}.toSet
    (unique1 ++ shared, unique2 ++ shared)
  }

  def sig[T,H](mh : MinHasher[H], x : Set[T]) =
    x.map{l => mh.init(l.toString)}.reduce{(a,b) => mh.plus(a,b)}

  def exactSimilarity[T](x : Set[T], y : Set[T]) = {
    (x & y).size.toDouble / (x ++ y).size
  }

  def approxSimilarity[T,H](mh : MinHasher[H], x : Set[T], y : Set[T]) = {
    val sig1 = sig(mh, x)
    val sig2 = sig(mh, y)
    mh.similarity(sig1, sig2)
  }

  "MinHasher32" should {
     "measure 0.5 similarity in 1024 bytes with < 0.1 error" in {
        testSimilarity(new MinHasher32(0.5, 1024), 0.5, 0.1)
     }
     "measure 0.8 similarity in 1024 bytes with < 0.05 error" in {
        testSimilarity(new MinHasher32(0.8, 1024), 0.8, 0.05)
     }
     "measure 1.0 similarity in 1024 bytes with < 0.01 error" in {
        testSimilarity(new MinHasher32(1.0, 1024), 1.0, 0.01)
     }
     "LSH with 0.5 threshold in 1024 bytes" in {
        testBuckets(new MinHasher32(0.5, 1024))
     }
     "LSH with 0.9 threshold in 1024 bytes" in {
        testBuckets(new MinHasher32(0.9, 1024))
     }
  }
}

package com.twitter.algebird

class MinHashMonoid(width : Int, seed : Int) extends Monoid[MinHashSignature] {
  //todo: we shouldn't really be abusing the CMSHash like this
  val hashes = {
    val r = new scala.util.Random(seed)
    (1 to width).map {CMSHash(r.nextInt, 0, Int.MaxValue)}    
  }

  val zero = MinHashSignature(0L, hashes.map{Int.MaxValue})
  def plus(l : MinHashSignature, r : MinHashSignature) = l ++ r

  def create(value : Long) = MinHashSignature(1L, hashes.map{h => h(value)})
}

case class MinHashSimilarity(val jaccard : Float, val leftCount : Long, val rightCount : Long) {
  def intersectionSize =  ((leftCount + rightCount) / (1 + jaccard)).toLong
  def unionSize = (leftCount + rightCount) - intersectionSize
  def cosine = unionSize.toDouble / (math.sqrt(leftCount) * math.sqrt(rightCount))
}

case class MinHashSignature(count : Long, values : Seq[Int]) {
  def ++(sig : MinHashSignature) = {
    val minValues = values.zip(sig.values).map{case (l,r) => l.min(r)}
    MinHashSignature(count + sig.count, minValues)
  }

  def similarityWith(sig : Signature) = {
    val matching = values.zip(sig.values).count{case (l,r) => l == r}
    val jaccard = matching.toFloat / values.size
    new MinHashSimilarity(jaccard, count, sig.count)
  }
}
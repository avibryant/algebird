package com.twitter.algebird

case class DyadicRange(maxValue : Long = Long.MaxValue) {
  val levels = math.ceil(math.log(maxValue) / math.log(2)).toInt

  def indicesForPoint(v : Long) = (1 to levels).map{level => (level, indexForPoint(v, level))}
  def indicesForRange(start : Long, end : Long)  : List[(Int,Long)] = indicesForRange(start, end, levels)

  def indexForPoint(v : Long, level : Int) = v >> (level - 1)
  def rangeForIndex(i : Long, level : Int) = (i << (level-1), ((i+1) << (level-1)) - 1)
  
  def indicesForRange(start : Long, end : Long, maxLevel : Int) : List[(Int,Long)] = {
    if(start > end) {
      Nil
    } else if(start == end) {
      List((1, start))
    } else {
      val startIndex = indexForPoint(start, maxLevel)
      val (a,b) = rangeForIndex(startIndex, maxLevel)
      if(a >= start) {
        if(b <= end) {
          List((maxLevel, startIndex)) ++ indicesForRange(start, a - 1, maxLevel) ++ indicesForRange(b + 1, end, maxLevel) 
        } else {
          indicesForRange(start, end, maxLevel - 1)
        }
      } else {
        val (a2, b2) = rangeForIndex(startIndex + 1, maxLevel)
        if(b2 <= end && a2 <= end) {
          List((maxLevel, startIndex + 1)) ++ indicesForRange(start, a2 - 1, maxLevel - 1) ++ indicesForRange(b2 + 1, end, maxLevel)
        } else {
          indicesForRange(start, end, maxLevel - 1)
        }
      }
    }
  }

}
/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.algebird

/**
 * Based on "A Framework for Clustering Massive-Domain Data Streams", C Aggarwal
 * http://charuaggarwal.net/cskrevise.pdf
 *
 * This isn't properly associative, but is hopefully stable enough in practice.
 *
 * @author Avi Bryant
 */

case class CMSClusters[K: Ordering](clusters: Seq[CMS[K]], params: CMSParams[K]) {
  def clusterIndexFor(keys: Seq[K]): Int = clusterIndexFor(CMSItems(keys.toList, params))

  def clusterIndexFor(cms: CMS[K]): Int = {
    //randomize the order we search the clusters. TODO: make this deterministic?
    val start = scala.util.Random.nextInt(clusters.size)

    0.until(clusters.size).map{ i =>
      val index = (start + i) % clusters.size
      index -> clusters(index).innerProduct(cms).estimate
    }.maxBy{ _._2 }
      ._1
  }
}

case class CMSClustersMonoid[K: Ordering: CMSHasher](clusters: Int, cmsMonoid: CMSMonoid[K]) {
  val zero = CMSClusters[K](1.to(clusters).map{ i => cmsMonoid.zero }, cmsMonoid.params)

  def plus(left: CMSClusters[K], right: CMSClusters[K]) = {
    val clusterAssignments = right.clusters.groupBy{ r => left.clusterIndexFor(r) }
    val newClusters = left.clusters.zipWithIndex.map{
      case (cms, i) =>
        clusterAssignments.getOrElse(i, Nil).foldLeft(cms){ (l, r) => cmsMonoid.plus(l, r) }
    }
    CMSClusters[K](newClusters, cmsMonoid.params)
  }

  def create(keys: Seq[K]) = plus(zero, CMSClusters[K](List(cmsMonoid.create(keys)), cmsMonoid.params))
}

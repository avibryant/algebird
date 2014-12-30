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
import scala.util.Random

/**
 * Based on "A Framework for Clustering Massive-Domain Data Streams", C Aggarwal
 * http://charuaggarwal.net/cskrevise.pdf
 *
 * This isn't properly associative, but is hopefully stable enough in practice.
 *
 * @author Avi Bryant
 */

case class CMSClusters[K](clusters: Seq[CMS[K]]) {
  def clusterIndexFor(cms: CMS[K]): Int =
    findClusterIndex{ _.innerProduct(cms) }

  def clusterIndexFor(keys: Seq[K]): Int =
    findClusterIndex{ cms => keys.map{ k => cms.frequency(k) }.reduce{ _ + _ } }

  def findClusterIndex(fn: CMS[K] => Approximate[Long]): Int =
    Random.shuffle(clusters.zipWithIndex)
      .maxBy{ case (cluster, i) => fn(cluster).estimate }
      ._2
}

case class CMSClustersMonoid[K: Ordering: CMSHasher](clusters: Int, cmsMonoid: CMSMonoid[K]) {
  val zero = CMSClusters[K](1.to(clusters).map{ i => cmsMonoid.zero })

  def plus(left: CMSClusters[K], right: CMSClusters[K]) = {
    val clusterAssignments = right.clusters.groupBy{ r => left.clusterIndexFor(r) }
    val newClusters = left.clusters.zipWithIndex.map{
      case (cms, i) =>
        clusterAssignments.getOrElse(i, Nil).foldLeft(cms){ (l, r) => cmsMonoid.plus(l, r) }
    }
    CMSClusters[K](newClusters)
  }

  def create(keys: Seq[K]) = plus(zero, CMSClusters[K](List(cmsMonoid.create(keys))))
}

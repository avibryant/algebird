package com.twitter.algebird

trait MonoidAggregatable[A,T] {
  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): Aggregator[A, B, C]

  def count(pred: T => Boolean) = monoidAggregate(Aggregator.count(pred))
}

trait Aggregatable[A,T] extends MonoidAggregatable[A,T] {
  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C]
  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]) = aggregate(aggregator)
}

trait Preparer[A, T, +This[A, T] <: Preparer[A, T, This]] extends MonoidAggregatable[A,T] {
  def map[U](fn: T => U): This[A, U]

  def flatMap[U](fn: T => TraversableOnce[U]): FlatPreparer[A, U]

  def lift[B, C](aggregator: Aggregator[T, B, C]): MonoidAggregator[A, Option[B], Option[C]] =
    monoidAggregate(aggregator.lift)

  def split[B1, B2, C1, C2](fn: This[A, T] => (Aggregator[A, B1, C1], Aggregator[A, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(this.asInstanceOf[This[A, T]])
    a1.join(a2)
  }
}

object Preparer {
  def apply[A]: SimplePreparer[A, A] = SimplePreparer[A, A](identity)
}

case class SimplePreparer[A, T](prepareFn: A => T)
  extends Preparer[A, T, SimplePreparer]
  with Aggregatable[A, T] {

  def map[U](fn: T => U) =
    SimplePreparer[A, U](fn.compose(prepareFn))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatPreparer[A, U](fn.compose(prepareFn))

  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)
}

case class FlatPreparer[A, T](prepareFn: A => TraversableOnce[T])
  extends Preparer[A, T, FlatPreparer] {
  def map[U](fn: T => U) =
    FlatPreparer { a: A => prepareFn(a).map(fn) }

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatPreparer { a: A => prepareFn(a).flatMap(fn) }

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)
}

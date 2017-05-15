package com.training.scalding.getstarted

import com.twitter.scalding._
import MapOperations._
import schemas._

class MapOperationsJob(args: Args) extends Job(args) {
  val kidsList = List(
    ("rcb", "virat,ab,gayle"),
    ("mi", "rohit,mallinga,pollad"),
    ("gl", "raina,bravo,jadeja")
  )

  /** FlatMapTo :-  */
  val pipe =
    IterableSource[(String, String)](kidsList, IPL_SCHEMA).read
      .mapToExp
      .debug
      .write(TextLine(args("output")))
}

package com.training.scalding.getstarted

import scala.language.implicitConversions

import cascading.pipe.Pipe

import com.twitter.scalding._

package object schemas {
  val IPL_SCHEMA = List('team, 'players)
  val INPUT_DATA = List(
    ("rcb", "virat,ab,gayle"),
    ("mi", "rohit,mallinga,pollad"),
    ("gl", "raina,bravo,jadeja")
  )

  val COMPARISON_SCHEMA = List('season, 'virat, 'sachin)
  val COMPARISON_DATA = List(
    ("season1", "NA", "mi"),
    ("season2", "rcb", "mi"),
    ("season6", "rcb", "NA")
  )
}

trait MapOperations {
  import Dsl._

  def pipe: Pipe

  /**
    * Map Operation :-
    *
    */
  def mapExp = pipe.map('team -> 'withSeason) { team: String =>  s"Season 6 - $team" }

  /**
    * MapTo Operation :-
    *
    */
  def mapToExp = pipe.mapTo('team -> 'withSeason) { team: String =>  s"Season 6 - $team" }

  /**
    * FlatMap Operation :-
    *
    *
    */
  def flatMapExp = pipe.flatMap('players -> 'player) { players: String =>  players.split(",") }

  /**
    * Input schema: WITH_DAY_SCHEMA
    * Output schema: EVENT_COUNT_SCHEMA
    * @return
    */
  def flatMapToExp = pipe.flatMapTo('players -> 'player) { players: String => players.split(",")}

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def unpivotExp = pipe.unpivot(('virat, 'sachin) -> ('team, 'player))

  /**
    * Input schema: WITH_DAY_SCHEMA
    * Output schema: EVENT_COUNT_SCHEMA
    * @return
    */
  def projectExp = pipe.project('team)

  /**
    * Input schema: WITH_DAY_SCHEMA
    * Output schema: EVENT_COUNT_SCHEMA
    * @return
    */
  def discardExp = pipe.discard('players)

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def insertExp = pipe.insert('points, 0)

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def limitExp = pipe.limit(1)

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def filterExp = pipe.filter('team) {team: String => team == "mi"}

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def sampleExp = pipe.sample(0.30)
}

object MapOperations {
  implicit def wrapRicpPipe(rp: RichPipe): MapOperationsWrapper = new MapOperationsWrapper(rp.pipe)
  implicit class MapOperationsWrapper(val pipe: Pipe) extends MapOperations with Serializable
}
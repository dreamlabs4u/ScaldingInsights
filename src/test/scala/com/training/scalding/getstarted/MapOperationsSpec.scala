package com.training.scalding.getstarted

import com.twitter.scalding.RichPipe
import com.twitter.scalding.bdd.BddDsl
import org.scalatest.{Matchers, FlatSpec}
import com.training.scalding.getstarted.schemas._
import scala.collection.mutable
import MapOperations._

class MapOperationsSpec extends FlatSpec with Matchers with BddDsl {
  it should "verify that the map operation can be performed to add additional fields to existing source data" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.mapExp
    } Then {
      buffer: mutable.Buffer[(String, String, String)] =>
        buffer.toList should equal (
          List(
                ("rcb","virat,ab,gayle","Season 6 - rcb"),
                ("mi","rohit,mallinga,pollad","Season 6 - mi"),
                ("gl","raina,bravo,jadeja","Season 6 - gl")
          )
        )
    }
  }

  it should "verify that the mapTo operation can be performed to transform the source data to new set of fields" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.mapToExp
    } Then {
      buffer: mutable.Buffer[String] =>
        buffer.toList should equal (List("Season 6 - rcb", "Season 6 - mi", "Season 6 - gl"))
    }
  }

  it should "verify that the FlapMap operation can be performed to flatten the Players Field" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.flatMapExp
    } Then {
      buffer: mutable.Buffer[(String, String)] =>
        buffer.toList.size should equal (9)
    }
  }

  it should "verify that the FlatMapTo operation can be performed to flatten the players field" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.flatMapToExp
    } Then {
      buffer: mutable.Buffer[String] =>
        buffer.toList should equal (List("virat", "ab",  "gayle","rohit","mallinga", "pollad", "raina", "bravo", "jadeja"))
    }
  }

  it should "verify that the UnPivot operation can be performed to unpivot the players field" in {
    Given {
      COMPARISON_DATA withSchema COMPARISON_SCHEMA
    } When {
      (src: RichPipe) => src.unpivotExp
    } Then {
      buffer: mutable.Buffer[(String, String, String)] =>
        buffer.toList should equal (
          List(
            ("season1","virat","NA"),
            ("season1","sachin","mi"),
            ("season2","virat","rcb"),
            ("season2","sachin","mi"),
            ("season6","virat","rcb"),
            ("season6","sachin","NA")
          )
        )
    }
  }

  it should "verify that the Project operation can be performed extract required fields from a pipe" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.projectExp
    } Then {
      buffer: mutable.Buffer[String] =>
        buffer.toList should equal (
          List("rcb", "mi", "gl")
        )
    }
  }

  it should "verify that the Discard operation can be performed create new pipe excepting the fields specified" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.discardExp
    } Then {
      buffer: mutable.Buffer[String] =>
        buffer.toList should equal (
          List("rcb", "mi", "gl")
        )
    }
  }

  it should "verify that the Inert operation can be performed create new pipe with new field and static value" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.insertExp
    } Then {
      buffer: mutable.Buffer[(String, String, String)] =>
        buffer.toList should equal (
          List(
            ("rcb","virat,ab,gayle","0"),
            ("mi","rohit,mallinga,pollad","0"),
            ("gl","raina,bravo,jadeja","0")
          )
        )
    }
  }

  it should "verify that the Limit operation can be performed create new pipe with specified number of rows" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.limitExp
    } Then {
      buffer: mutable.Buffer[(String, String)] =>
        buffer.toList should equal (
          List(
            ("rcb","virat,ab,gayle")
          )
        )
    }
  }

  it should "verify that the Filter operation can be performed filter data in the pipe based on the condition" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.filterExp
    } Then {
      buffer: mutable.Buffer[(String, String)] =>
        buffer.toList should equal (
          List(
            ("mi","rohit,mallinga,pollad")
          )
        )
    }
  }

  it should "verify that the Smple operation can be performed Take a sample percentage of the original dataset" in {
    Given {
      INPUT_DATA withSchema IPL_SCHEMA
    } When {
      (src: RichPipe) => src.sampleExp
    } Then {
      buffer: mutable.Buffer[(String, String)] =>
        buffer.toList.size should equal (1)
    }
  }


}
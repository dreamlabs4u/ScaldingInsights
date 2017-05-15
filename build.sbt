name := "ScaldingInsights" // name of project

version := "1.0.0" // version of project  

scalaVersion := "2.11.8" // scala version

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.11" % "provided",               // dependency for scala specs2
  "com.twitter" % "scalding-core_2.11" % "0.15.0" % "provided",        // dependency for twitter scalding
  "org.apache.hadoop" % "hadoop-core" % "0.20.2" % "provided",     // dependency for hadoop
  "org.scalatest" % "scalatest_2.11" % "2.2.6"                    // For Scala Test
)

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

name := "ScaldingInsights" // name of project

version := "1.0.0" // version of project  

scalaVersion := "2.10.2" // scala version

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.11" % "provided",               // dependency for scala specs2
  "com.twitter" % "scalding-core_2.10" % "0.15.0" % "provided",        // dependency for twitter scalding
  "org.apache.hadoop" % "hadoop-core" % "0.20.2" % "provided"     // dependency for hadoop
)

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

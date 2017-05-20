name := "MediaMath"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.1.0",
  "org.scalatest" % "scalatest_2.11" % "3.0.1"
)

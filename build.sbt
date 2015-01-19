name := "spark-LSH"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0",
	"org.apache.commons" % "commons-math3" % "3.2",
  "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
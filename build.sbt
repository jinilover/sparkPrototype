name := "sparkPrototype"

version := "1.0"

scalaVersion := "2.10.4"

//val scalazVersion = "7.0.6"
val scalazVersion = "7.1.0"

val sparkVersion = "1.3.0"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.scalaz" %% "scalaz-core" % scalazVersion,
    "org.scalaz" %% "scalaz-effect" % scalazVersion,
    "org.scalaz" %% "scalaz-typelevel" % scalazVersion,
    "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion % "test"
  )
}

    
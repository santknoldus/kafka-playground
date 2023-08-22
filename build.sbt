ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-client",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "3.5.1",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2"
    ),
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
    libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.1.2",
    libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.7",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.2" % "provided",
    libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1"

  )

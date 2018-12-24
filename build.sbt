name := "myapp"
version := "0.0.1"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

name := "dip22-ex3"
version := "1.0"
scalaVersion := "2.12.16"
val SparkVersion: String = "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % SparkVersion

// suppress all log messages for setting up the Spark Session
javaOptions += "-Dlog4j.configurationFile=project/log4j.properties"

// to avoid java.nio.file.NoSuchFileException at the end of execution
run / fork := true

name := "Spark TPC-H Queries"

version := "1.1-SNAPSHOT"

scalaVersion := "2.11.8"

fork := true

javaOptions += "-Xmx4G"

enablePlugins(JavaAppPackaging)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.kududb" %% "kudu-spark" % "1.0.0-SNAPSHOT"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.1"

libraryDependencies += "org.kududb" % "kudu-client" % "1.0.0-SNAPSHOT"

libraryDependencies += "commons-cli" % "commons-cli" % "1.3"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.4.0"

libraryDependencies += "org.ini4j" % "ini4j" % "0.5.4"

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"


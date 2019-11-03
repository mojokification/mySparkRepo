name := "SparkDataFrame"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies +="com.typesafe" % "config" % "1.3.2" //Scala based plugin to make use of externalized parameters.
libraryDependencies +="org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies +="org.apache.spark" %% "spark-sql" % "2.3.0"


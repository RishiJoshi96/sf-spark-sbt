name := "sf-spark"

version := "1.0"

scalaVersion := "2.12.17"

val sparkVersion = "3.3.1"

resolvers ++= Seq(
"apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.12" % sparkVersion,
"org.apache.spark" % "spark-sql_2.12" % sparkVersion,
"net.snowflake" % "snowflake-ingest-sdk" % "0.10.8",
"net.snowflake" % "snowflake-jdbc" % "3.13.26",
"net.snowflake" % "spark-snowflake_2.12" % "2.11.0-spark_3.3"
)







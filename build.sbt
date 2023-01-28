name := "safescheduler"

version := "0.1"

scalaVersion := "2.11.12"

autoCompilerPlugins := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "mysql" % "mysql-connector-java" % "8.0.32"
)
scalacOptions ++= Seq(
  "-P:flowframe:lang:purpose"
)

addCompilerPlugin("com.facebook" % "flowframe_2.11.12" % "0.1-SNAPSHOT")

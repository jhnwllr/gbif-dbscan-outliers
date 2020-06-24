name := "gbif-dbscan-outliers"

version := "0.1"

scalaVersion := "2.11.1"

// libraryDependencies += "org.scalanlp" % "nak" % "1.2.1"
//libraryDependencies += "com.github.haifengl" %% "smile-scala" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % Provided

libraryDependencies += "org.scalanlp" % "nak_2.11" % "1.3"



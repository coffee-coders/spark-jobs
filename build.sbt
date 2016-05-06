mainClass in Compile := Some("CrimesCountV3")        
name := "spark-jobs"
organization := "unicamp"
version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1" % "provided"
  , "org.apache.spark" % "spark-mllib_2.11" % "1.6.1" % "provided"
  , "com.github.melrief" % "purecsv_2.11" % "0.0.6"
//  , compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

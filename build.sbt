name := "KafkaMessageFormatter"

version := "0.1"

scalaVersion := "2.13.4"

resolvers ++= Seq[Resolver](
  "BintrayJCenter" at "https://jcenter.bintray.com",
  "OSS Sonatype" at "https://repo1.maven.org/maven2/",
  "mvn repository" at "https://mvnrepository.com/artifact/",
  "mvn snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies += "org.apache.kafka" %% "kafka" % "3.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test
libraryDependencies += "org.scalatestplus" %% "mockito-3-4" % "3.2.9.0" % Test

mainClass in assembly := None

// Assembly settings
target in assembly := file("target/jars")
assemblyMergeStrategy in assembly := {
  //The following line is needed to find configuration for AKKA. It was failing with NoClassFound without this.
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

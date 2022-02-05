import sbt.Resolver

logLevel := sbt.Level.Info


addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")


resolvers ++= Seq(
  Resolver.mavenLocal
)

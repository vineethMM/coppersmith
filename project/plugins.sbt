resolvers ++= Seq(
  Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]")),
  "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

val uniformVersion = "1.4.0-20151122210042-f94f9b6"

addSbtPlugin("au.com.cba.omnia" % "uniform-core"       % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-thrift"     % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-assembly"   % uniformVersion)

organization := "com.ansvia.nsqie"

name := "nsqie"

description := ""

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.9.2"



resolvers ++= Seq(
	"Sonatype Releases" at "https://oss.sonatype.org/content/groups/scala-tools",
	"typesafe repo"   at "http://repo.typesafe.com/typesafe/releases",
	"Ansvia release repo" at "http://scala.repo.ansvia.com/releases",
	"Ansvia snapshot repo" at "http://scala.repo.ansvia.com/nexus/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
    "org.specs2" % "specs2_2.9.2" % "1.12.4.1",
    "net.databinder.dispatch" %% "dispatch-core" % "0.9.5",
    "com.twitter" %% "finagle-core" % "6.5.2",
    "com.twitter" % "finagle-http_2.9.2" % "6.5.2",
    "net.liftweb" % "lift-json_2.9.2" % "2.5.1",
    "com.ansvia" % "ansvia-commons" % "0.0.7",
    "ch.qos.logback" % "logback-classic" % "1.0.7"
)

EclipseKeys.withSource := true


publishTo <<= version { (v:String) =>
    val ansviaRepo = "http://scala.repo.ansvia.com/nexus"
    if(v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at ansviaRepo + "/content/repositories/snapshots")
    else
        Some("releases" at ansviaRepo + "/content/repositories/releases")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

crossPaths := false

pomExtra := (
  <url>http://ansvia.com</url>
  <developers>
    <developer>
      <id>anvie</id>
      <name>Robin Sy</name>
      <url>http://www.mindtalk.com/u/robin</url>
    </developer>
  </developers>)

import _root_.sbt.Keys._

organization := "org.scorexfoundation"

name := "iodb"

version := "0.1.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.mapdb" % "mapdb" % "3.0.1",
  "com.google.guava" % "guava" % "19.0",
  "org.consensusresearch" %% "scrypto" % "1.2.0-RC3",
  "org.scalatest" %% "scalatest" % "2.+" % "test",
  "org.scalactic" %% "scalactic" % "2.+" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.+" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.rocksdb" % "rocksdbjni" % "4.5.1" % "test",
  "org.slf4j" % "slf4j-api" % "1.+",
  "ch.qos.logback" % "logback-classic" % "1.+"
)

licenses := Seq("CC0" -> url("https://creativecommons.org/publicdomain/zero/1.0/legalcode"))

homepage := Some(url("https://github.com/ScorexProject/iodb"))

resolvers ++= Seq("Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "SonaType" at "https://oss.sonatype.org/content/groups/public",
  "Typesafe maven releases" at "http://repo.typesafe.com/typesafe/maven-releases/")

publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra :=
  <scm>
    <url>git@github.com:ScorexProject/iodb.git</url>
    <connection>scm:git:git@github.com:ScorexProject/iodb.git</connection>
  </scm>
    <developers>
      <developer>
        <id>kushti</id>
        <name>Alexander Chepurnoy</name>
        <url>http://chepurnoy.org/</url>
      </developer>
      <developer>
        <id>jan</id>
        <name>Jan Kotek</name>
      </developer>

    </developers>

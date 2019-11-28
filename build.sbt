import sbt._

name := "ycsb"

version := "0.1"

scalaVersion := "2.12.10"

maintainer := "rchaki@joveo.com"

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("com.joveox.ycsb.jobs.Main")

resolvers += ("ImageJ Public Repository" at "http://maven.imagej.net/content/repositories/public/")
  .withAllowInsecureProtocol(true)

val commonJVMOptions = Seq(
  "-J-XX:+UseParallelOldGC",
  "-J-XX:+UseParallelGC",
  "-J-XX:+UseStringDeduplication",
  "-J-Xloggc:gc.log",
  "-J-XX:+HeapDumpOnOutOfMemoryError",
)

scalacOptions in ThisBuild ++= Seq(
  "-J-Xss8M",
  "-deprecation",
  "-encoding", "UTF-8",
  "-language:experimental.macros",
  "-feature",
  "-unchecked",
  "-Xmacro-settings:materialize-derivations"
)


javaOptions in Universal ++= ( commonJVMOptions ++ Seq("-J-Xmx400G", "-J-Xss256k") )

libraryDependencies ++= Seq(
  "com.datastax.oss" % "java-driver-core" % "4.2.2",
  "com.datastax.oss" % "java-driver-query-builder" % "4.2.2",

  "com.univocity" % "univocity-parsers" % "2.8.3",

  "io.monix" %% "monix" % "3.0.0",

  "com.github.brianfrankcooper.YCSB" % "core" % "0.15.0" ,

  "com.beachape" %% "enumeratum" % "1.5.13",
  "com.github.pureconfig" %% "pureconfig" % "0.12.1",
  "com.github.pureconfig" %% "pureconfig-enumeratum" % "0.12.1",

  "com.lihaoyi" %% "upickle" % "0.8.0",

  "org.apache.logging.log4j" % "log4j-core" % "2.12.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
)




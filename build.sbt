ThisBuild / organization := "lasp"
ThisBuild / scalaVersion := "2.12.8"

val artifactory = "http://web-artifacts.lasp.colorado.edu/artifactory/"

val latisVersion    = "3.0.0-SNAPSHOT"
val sparkVersion    = "2.4.0"
val jettyVersion    = "9.4.11.v20180605"
val geotoolsVersion = "20.2"
val fs2Version      = "1.0.2"

lazy val `latis3-core` = ProjectRef(file("../latis3"), "core")

lazy val hylatis = (project in file("."))
  .dependsOn(`latis3-core`)
  .settings(commonSettings)
  .settings(
    name := "latis-hylatis",
    libraryDependencies ++= Seq(
      // "io.latis-data"           %% "latis-core"      % latisVersion,
      // "io.latis-data"           %% "latis-spark"     % latisVersion,
      "org.eclipse.jetty"           % "jetty-server"    % jettyVersion,
      "org.eclipse.jetty"           % "jetty-servlet"   % jettyVersion,
      "org.geotools"                % "gt-main"         % geotoolsVersion,
      "org.geotools"                % "gt-epsg-hsql"    % geotoolsVersion,
      "org.geotools"                % "gt-api"          % geotoolsVersion,
      "org.geotools"                % "gt-referencing"  % geotoolsVersion,
      "org.apache.commons"          % "commons-math3"   % "3.6.1",
      "io.findify"                 %% "s3mock"          % "0.2.4",
      "edu.ucar"                    % "cdm"             % "5.0.0-SNAPSHOT" classifier "s3+hdfs",
      "edu.ucar"                    % "httpservices"    % "5.0.0-SNAPSHOT",
      "org.apache.spark"           %% "spark-sql"       % sparkVersion,
      "com.amazonaws"               % "aws-java-sdk-s3" % "1.11.489",
      "co.fs2"                     %% "fs2-core"        % fs2Version,
      "co.fs2"                     %% "fs2-io"          % fs2Version,
    ),
    updateOptions := updateOptions.value.withGigahorse(false),
    resolvers ++= Seq(
      "osgeo" at "http://download.osgeo.org/webdav/geotools",
      "Boundless" at "http://repo.boundlessgeo.com/main",
      "Artifactory External Snapshots" at artifactory + "ext-snapshot-local",
      "Unidata" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases",
      "Unidata Snaphots" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-snapshots"
    ),
    assembly / mainClass := Some("latis.server.HylatisServer"),
    assembly / assemblyMergeStrategy := {
      case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".tsml" => MergeStrategy.first
      case "latis.properties"                            => MergeStrategy.first
      case PathList("tec", "uom", "se", "format", "messages.properties") => MergeStrategy.first
      case PathList("META-INF", "jdom-info.xml")         => MergeStrategy.discard
      case x =>
        val strategy = (assemblyMergeStrategy in assembly).value
        strategy(x)
    },
    // We can exclude the Scala libraries in the JAR we submit via
    // spark-submit.
    assembly / assemblyOption := {
      val orig = (assemblyOption in assembly).value
      orig.copy(includeScala = false)
    },
    // Disable tests when assembling.
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.apache.http.**" -> "shade.@1").inAll
    )
    // Enable sbt to find scala files (dataset descriptors) in /src/main/resources/datasets/
    //unmanagedSourceDirectories in Compile += (resourceDirectory in Compile).value / "datasets"
  )

lazy val commonSettings = compilerFlags ++ Seq(
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"          % "junit"     % "4.12"  % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-unchecked",
    "-Xfuture",
    "-Xlint:-unused,_",
    "-Ypartial-unification",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-value-discard"
  ),
  Compile / console / scalacOptions --= Seq(
    "-Ywarn-unused"
  )
)

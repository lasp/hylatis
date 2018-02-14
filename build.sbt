ThisBuild / organization := "lasp"
ThisBuild / scalaVersion := "2.11.8"

val latisVersion = "3.0.0-SNAPSHOT"
//val jerseyVersion = "1.19.4"
val jettyVersion  = "9.4.7.v20170914"

lazy val hylatis = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "latis-hylatis",
    libraryDependencies ++= Seq(
      "io.latis-data"     %% "latis-core"    % latisVersion,
      "io.latis-data"     %% "latis-spark"   % latisVersion,
      "org.eclipse.jetty" % "jetty-server"   % jettyVersion,
      "org.eclipse.jetty" % "jetty-servlet"  % jettyVersion,
      "org.geotools"      % "gt-main"        % "18.2",
      "org.geotools"      % "gt-epsg-hsql"   % "18.2",
      "org.geotools"      % "gt-api"         % "18.2",
      "org.geotools"      % "gt-referencing" % "18.2"
    ),
    resolvers ++= Seq(
      "Boundless" at "http://repo.boundlessgeo.com/main"
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".tsml" => MergeStrategy.first
      case "latis.properties"                            => MergeStrategy.first
      case x =>
        val strategy = (assemblyMergeStrategy in assembly).value
        strategy(x)
    },
    // We can exclude the Scala libraries in the JAR we submit via
    // spark-submit.
    assemblyOption in assembly := {
      val orig = (assemblyOption in assembly).value
      orig.copy(includeScala = false)
    },
    // Enable sbt to find scala files (dataset descriptors) in /src/main/resources/datasets/
    unmanagedSourceDirectories in Compile += (resourceDirectory in Compile).value / "datasets"
  )
  
lazy val commonSettings = compilerFlags ++ Seq(
  Compile / compile / wartremoverWarnings ++= Warts.allBut(
    Wart.Any,         // false positives
    Wart.Nothing,     // false positives
    Wart.Product,     // false positives
    Wart.Serializable // false positives
  ),
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"            % "junit"           % "4.12"      % Test,
    "com.novocode"     % "junit-interface" % "0.11"      % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)


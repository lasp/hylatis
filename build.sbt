lazy val root = (project in file(".")).
  //dependsOn(latis3).
  settings(
    name := "latis-hylatis",
    scalaVersion := "2.11.8",
    scalacOptions ++= scalacCommon,
    scalacOptions in (Compile, compile) ++=
      scalacCommon ++ Seq(
        "-Ywarn-unused",
        "-Ywarn-unused-import"
      ),
    libraryDependencies ++= Seq(
      //"com.sun.jersey"    % "jersey-core"     % jerseyVersion % "runtime",
      //"com.sun.jersey"    % "jersey-server"   % jerseyVersion % "runtime",
      "io.lambdata"       %% "latis-core"     % latisVersion,
      "io.lambdata"       %% "latis-spark"    % latisVersion,
      "junit"             % "junit"           % "4.+"  % Test,
      "com.novocode"      % "junit-interface" % "0.11" % Test,
      "org.eclipse.jetty" % "jetty-server"    % jettyVersion,
      "org.eclipse.jetty" % "jetty-servlet"   % jettyVersion
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

val latisVersion = "3.0.0-SNAPSHOT"
val jerseyVersion = "1.19.4"
val jettyVersion  = "9.4.7.v20170914"

val scalacCommon =
  Seq(
    "-encoding", "UTF-8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Ywarn-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture"
  )

//lazy val latis3         = ProjectRef(file("../latis3"), "latis3")
//lazy val `latis-spark` = ProjectRef(file("../latis3/spark"), "spark")

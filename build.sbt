lazy val root = (project in file(".")).
  dependsOn(latis, latis_spark).
  settings(
    name := "latis-hylatis",
    scalaVersion := "2.11.8",
    libraryDependencies += "com.sun.jersey" % "jersey-core"   % "1.19.4" % "runtime",
    libraryDependencies += "com.sun.jersey" % "jersey-server" % "1.19.4" % "runtime",
    webappWebInfClasses := true,
    containerArgs := Seq("--path", "/latis-hylatis")
  )
enablePlugins(JettyPlugin)

lazy val latis       = ProjectRef(file("../latis"), "latis")
lazy val latis_spark = ProjectRef(file("../latis-spark"), "latis-spark")

EclipseKeys.skipProject in latis := true

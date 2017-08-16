/*
 * Load latis.properties to get
 *   log.dir
 *   log.file
 */
def props = new Properties()
this.getClass().getResource( 'latis.properties' ).withInputStream {
  stream -> props.load(stream)
}

//Determine the log file name based on latis.properties.
def dir = props["log.dir"]
def file = props["log.file"]
def logFile = (dir && file) ? dir + File.separator + file : file

//Define the log message pattern.
//Note that LaTiS "log" datasets are configured to parse this format.
def msgPattern = "[%d{yyyy-MM-dd'T'HH:mm:ss.SSS, GMT} %-5level %logger{36} (%thread\\)] %msg%n"

appender("CONSOLE", ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = msgPattern
  }
}

//Define file-based appenders if logFile is defined.
if (logFile) {

  appender("FILE", FileAppender) {
    file = ${logFile}
    append = true
    encoder(PatternLayoutEncoder) {
      pattern = msgPattern
    }
  }

  appender("DAILYFILE", RollingFileAppender) {
    append = true
    rollingPolicy(TimeBasedRollingPolicy) {
      fileNamePattern = "${logFile}-%d{yyyy-MM-dd}"
      maxHistory = 30
    }
    encoder(PatternLayoutEncoder) {
      pattern = msgPattern
    }
  }
}

//Define the root logger.
if (logFile) {
  root(INFO, ["DAILYFILE"])
} else {
  root(INFO, ["CONSOLE"])
}

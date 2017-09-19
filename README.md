# latis-hylatis

## Dependencies

The following repositories must be cloned in sister directories:

- [latis](http://stash.lasp.colorado.edu/projects/WEBAPPS/repos/latis)
- [latis-spark](http://stash.lasp.colorado.edu/projects/WEBAPPS/repos/latis-spark)

## Running

### Standalone

Running `sbt run` will run `latis-hylatis` using embedded Jetty.

### On Spark

After setting the configuration of the Spark dependency in the
`latis-spark` build to `provided`, running `sbt assembly` will produce
a JAR than can be submitted using the `spark-submit` script.

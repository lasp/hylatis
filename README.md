# latis-hylatis

## Dependencies

The following repositories must be cloned in sister directories:

- [latis3-beta](http://stash.lasp.colorado.edu/projects/WEBAPPS/repos/latis3-beta)

## Running

### Standalone

Running `sbt run` will run `latis-hylatis` using embedded Jetty.

### On Spark

After setting the configuration of the Spark dependency in the
`latis3-beta` build to `provided`, running `sbt assembly` will produce
a JAR than can be submitted using the `spark-submit` script.

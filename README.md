# latis-hylatis

## Dependencies

The following repository must be cloned in sister directories:

- [latis3](http://stash.lasp.colorado.edu/projects/WEBAPPS/repos/latis3)

## Running

### Standalone

This setup uses S3Mock to mimic the S3 storage. It expects a `/data/s3` 
directory with the `hylatis-hysics-001` directory (bucket) containing 
the `des_veg_cloud` data. (See kestrel)

Start S3Mock then run the HylatisServer (which uses embedded Jetty).
`sbt run` will provide options to run each of those.

To reduce the volume of data, set the `imageCount` property in `latis.properties`
to a small number before running the server. (The default is 4200.) 
You can also limit the number of pixels along the slit by adding a selection 
on the `ix` index. e.g. `ix<3`.

### On Spark

After setting the configuration of the Spark dependency in the
`latis3` build to `provided`, running `sbt assembly` will produce
a JAR than can be submitted using the `spark-submit` script.

### Image request

http://localhost:8090/latis-hylatis/dap/hysics.png?rgbPivot(wavelength,630.87,531.86,463.79)
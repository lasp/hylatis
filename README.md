# latis-hylatis

## Dependencies

[LaTiS][latis] must be cloned in a sister directory called `latis`.

[latis]: https://github.com/latis-data/latis

## Running

This project is configured to use [xsbt-web-plugin][xsbt] for running
the server in Jetty.

To run the server, run the `jetty:start` task. Jetty can be stopped
with `jetty:stop`.

[xsbt]: https://github.com/earldouglas/xsbt-web-plugin

# clickhousetest

A library to manage an ephemeral Clickhouse server for Go tests.
Similar to [postgrestest](https://github.com/zombiezen/postgrestest/) but for clickhouse.

#### TODO
[x] Add options for "exec mode" and "normal mode". In "exec mode" the library will run and maintain the ephemeral clickhouse server. In "normal mode" (if using docker-compose, some pipeline, etc) we simply connect to supplied clickhouse server. 

[x] Figure out overwriting default clickhouse server options (slightly non trivial directory structure with restricted folder permissions). Look for unused port and assign that

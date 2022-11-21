<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>

### ClickhouseTest

Provides a test harness with a managed temporary clickhouse server.
Inspired by [postgrestest](https://github.com/zombiezen/postgrestest/).

### Installation

##### `go get github.com/kalbhor/clickhousetest`

For temporary server, clickhouse binary needs to be available in your PATH.

### Server

Server holds the clickhouse connections and metadata (such as db directory, path, etc) and manages the start & stop of the ephemeral server.

#### Server Options

Simple options to disable temporary server. If disabled, relevant clickhouse options need to be passed to connect to an existing clickhouse server.
Useful when transitioning to docker based pipelines or other setups.

```go
type Options struct {
	NoExec  bool
	DBOptions clickhouse.Options
}
```

### Usage

#### `Start` 
Creates ephemeral CH server and makes connections.

#### `NewDatabase` 
Creates a random database and returns a connection to it.

#### `Stop` 
Does the cleanup associated with closing ephemeral server.

```go
package examples

import (
	"context"
	"testing"

	"github.com/kalbhor/clickhousetest"
)

var createQuery = `
CREATE TABLE instrument (
    id UInt64,
    timestamp DateTime('Asia/Kolkata'),
    price Int64,
)
ENGINE = ReplacingMergeTree()
ORDER BY (id, timestamp);
`

func Example() {
	var t *testing.T

	// Start up the clickhouse server. Do this once per test run.
	ctx := context.Background()
	srv, err := clickhousetest.Start(ctx, clickhousetest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Stop(); err != nil {
			t.Error(err)
		}
	})

	// Each of your tests can have their own database
	t.Run("test1", func(t *testing.T) {
		conn, err := srv.NewDatabase(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, createQuery); err != nil {
			t.Fatal(err)
		}
		// ...
	})

}
```
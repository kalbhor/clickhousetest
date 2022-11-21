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

	// Each of your subtests can have their own database:
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

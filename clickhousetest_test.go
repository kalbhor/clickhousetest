package clickhousetest

import (
	"context"
	"testing"
)

var (
	createTable = `
CREATE TABLE IF NOT EXISTS ticks (
    token UInt64,
    timestamp DateTime('Asia/Kolkata'),
    price Int64,
)
ENGINE = ReplacingMergeTree()
ORDER BY (token, timestamp);
`
)

type Result struct {
	DbName string `ch:"db_name"`
}

func TestStart(t *testing.T) {
	server, err := Start(context.Background())
	defer server.Stop()
	if err != nil {
		t.Fatalf(err.Error())
	}

}

func TestNewDatabase(t *testing.T) {
	ctx := context.Background()
	server, err := Start(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}

	defer server.Stop()

	conn, err := server.NewDatabase(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var rs []Result
	err = conn.Select(ctx, &rs, "SELECT currentDatabase() AS db_name;")
	if err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("connected to new db : %s", rs)
	err = conn.Exec(ctx, createTable)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

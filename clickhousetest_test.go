package clickhousetest

import (
	"context"
	"os/exec"
	"testing"
)

var (
	createTable = `
CREATE TABLE instruments (
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
	server, err := Start(context.Background(), Options{})
	defer server.Stop()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestStartNoExec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmdStr := `docker run -p 127.0.0.1:8123:8123 -p 127.0.0.1:9000:9000 
	--ulimit nofile=262144:262144 -d clickhouse/clickhouse-server:latest`
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", cmdStr)

	err := cmd.Start()
	if err != nil {
		t.Errorf("could not start docker container : %v", err)
	}

	server, err := Start(context.Background(), Options{NoExec: true})
	defer server.Stop()
	if err != nil {
		t.Errorf("could not start server : %v", err)
	}

	conn, err := server.NewDatabase(ctx)
	if err != nil {
		t.Errorf("could not create new db : %v", err)
	}

	var rs []Result
	err = conn.Select(ctx, &rs, "SELECT currentDatabase() AS db_name;")
	if err != nil {
		t.Errorf("could not select current db : %v", err)
	}

	t.Logf("connected to new db : %s", rs)
	err = conn.Exec(ctx, createTable)
	if err != nil {
		t.Errorf("could not create table : %v", err)
	}
}

func TestNewDatabase(t *testing.T) {
	ctx := context.Background()
	server, err := Start(ctx, Options{})
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

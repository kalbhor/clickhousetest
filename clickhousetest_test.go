package clickhousetest

import (
	"context"
	"testing"
)

func TestStart(t *testing.T) {
	srv, err := New()
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = srv.Start(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = srv.Stop()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

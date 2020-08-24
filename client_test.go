package dalga

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient(t *testing.T) {

	c := make(chan string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		defer r.Body.Close()

		c <- buf.String()
		w.Write([]byte("OK"))
	}))

	config := DefaultConfig
	config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String()
	config.MySQL.SkipLocked = false
	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer func() {
		cancel()
		<-d.NotifyDone()
	}()

	callCtx := context.Background()

	clnt := NewClient("http://" + lis.Addr())

	t.Run("get nonexistent", func(t *testing.T) {
		_, err := clnt.Get(callCtx, "what", "who")
		if err != ErrNotExist {
			t.Fatal("expected ErrNotExist")
		}
	})

	t.Run("schedule", func(t *testing.T) {
		if j, err := clnt.Schedule(callCtx, "when", "where", MustWithIntervalString("PT1M")); err != nil {
			t.Fatal(err)
		} else if j.Body != "where" {
			t.Fatalf("unexpected body: %s", j.Body)
		}
	})

	t.Run("get", func(t *testing.T) {
		if j, err := clnt.Get(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		} else if j.Body != "where" {
			t.Fatalf("unexpected body: %s", j.Body)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		if err := clnt.Cancel(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		}
		if _, err := clnt.Get(callCtx, "when", "where"); err != ErrNotExist {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("idempotent cancel", func(t *testing.T) {
		if err := clnt.Cancel(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		}
	})

}

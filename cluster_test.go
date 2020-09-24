package dalga

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {

	called := make(chan struct {
		body     string
		instance string
	})
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- struct {
			body     string
			instance string
		}{buf.String(), r.Header.Get("dalga-instance")}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", endpoint)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	numInstances := 3
	var client *Client
	instances := make([]*Dalga, numInstances)
	for i := 0; i < numInstances; i++ {
		config := DefaultConfig
		config.MySQL.SkipLocked = false
		config.MySQL.Table = "test_cluster"
		config.Jobs.FixedIntervals = true
		config.Jobs.ScanFrequency = 100 * time.Millisecond
		config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"
		config.Listen.Port = 34100 + i

		d, lis, cleanup := newDalga(t, config)
		instances[i] = d
		if i == 0 {
			client = NewClient("http://" + lis.Addr())
		}
		defer cleanup()
	}

	ctx, cancel := context.WithCancel(context.Background())
	for _, inst := range instances {
		go inst.Run(ctx)
	}
	defer func() {
		cancel()
		for _, inst := range instances {
			<-inst.NotifyDone()
		}
	}()

	// This is a fairly crude test which attempts to ensure that,
	// with several instances running at once, two instances will not
	// grab the same job and execute it.  It's hardly a Jepsen test,
	// more of a basic sanity check.
	t.Run("run at most once", func(t *testing.T) {

		start := time.Now().Add(time.Second)
		jobCount := 100
		received := make(map[string]bool, jobCount)
		for i := 0; i < jobCount; i++ {
			key := fmt.Sprintf("job%d", i)
			_, err := client.Schedule(ctx, "apple", key, WithFirstRun(start), WithOneOff())
			if err != nil {
				t.Fatal(err)
			}
			received[key] = false
		}

		for i := 0; i < jobCount; i++ {
			rcv := <-called
			if ok := received[rcv.body]; ok {
				t.Errorf("Received job %s twice!", rcv)
			}
			received[rcv.body] = true
		}

		for key, ok := range received {
			if !ok {
				t.Errorf("Did not receive job %s", key)
			}
		}

		time.Sleep(time.Second)

	})

	t.Run("distributed amongst instances", func(t *testing.T) {

		start := time.Now().Add(time.Second)
		jobCount := 99
		countsByInstance := map[string]int{}
		for i := 0; i < jobCount; i++ {
			key := fmt.Sprintf("job%d", i)
			_, err := client.Schedule(ctx, "banana", key, WithFirstRun(start), WithOneOff())
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < jobCount; i++ {
			rcv := <-called
			countForInstance := countsByInstance[rcv.instance]
			countsByInstance[rcv.instance] = countForInstance + 1
		}

		t.Logf("Counts by instance: %+v", countsByInstance)

		if len(countsByInstance) != len(instances) {
			t.Fatalf("Expected each instance to have done some jobs.")
		}

		time.Sleep(time.Second)

	})

}

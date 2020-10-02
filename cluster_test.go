package dalga // nolint: testpackage

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

type callResult struct {
	body     string
	instance string
}

func TestCluster(t *testing.T) {
	const numInstances = 10
	const jobCount = 100

	called := make(chan callResult)
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- callResult{
			body:     buf.String(),
			instance: r.Header.Get("dalga-instance"),
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", endpoint)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	instances := make([]*Dalga, numInstances)
	for i := 0; i < numInstances; i++ {
		config := DefaultConfig
		config.MySQL.SkipLocked = false
		config.MySQL.Table = "test_cluster"
		config.Jobs.FixedIntervals = true
		config.Jobs.ScanFrequency = 100 * time.Millisecond
		config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"
		config.Listen.Port = 34100 + i

		d, _, cleanup := newDalga(t, config)
		instances[i] = d
		defer cleanup()
	}
	client := NewClient("http://" + instances[0].listener.Addr().String())

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
		start := time.Now()
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
	})

	t.Run("distributed amongst instances", func(t *testing.T) {
		start := time.Now()
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
	})
}

func TestClusterSuccession(t *testing.T) {
	const numInstances = 3
	instances := make([]*Dalga, numInstances)
	ctxes := make([]context.Context, numInstances)
	cancels := make([]func(), numInstances)
	for i := 0; i < numInstances; i++ {
		ctxes[i], cancels[i] = context.WithCancel(context.Background())
	}
	killStart, killDone := make(chan string), make(chan string)
	kill := func(w http.ResponseWriter, r *http.Request) {
		instance := r.Header.Get("dalga-instance")
		killStart <- instance
		killDone <- instance
		w.WriteHeader(200)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kill", kill)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var client *Client
	for i := 0; i < numInstances; i++ {
		config := DefaultConfig
		config.MySQL.SkipLocked = false
		config.MySQL.Table = "test_cluster_succession"
		config.Jobs.FixedIntervals = true
		config.Jobs.ScanFrequency = 100 * time.Millisecond
		config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"
		config.Listen.Port = 34200 + i

		d, lis, cleanup := newDalga(t, config)
		instances[i] = d
		if i == 0 {
			client = NewClient("http://" + lis.Addr())
		}
		defer cleanup()
	}

	for i, inst := range instances {
		go inst.Run(ctxes[i])
	}
	defer func() {
		for i, inst := range instances {
			if fn := cancels[i]; fn != nil {
				fn()
			}
			<-inst.NotifyDone()
		}
	}()

	t.Run("reclaim jobs", func(t *testing.T) {
		start := time.Now()
		_, err := client.Schedule(context.Background(), "kill", "bill", WithFirstRun(start), WithOneOff())
		if err != nil {
			t.Fatal(err)
		}

		// An instance is going to run the job.
		var doomedInstance string
		select {
		case doomedInstance = <-killStart:
		case <-time.After(time.Second * 2):
			t.Fatal("Didn't get the kill start")
		}

		// Now we kill it before the cycle of action completes.
		t.Logf("Killing instance %s", doomedInstance)
		id, err := strconv.ParseUint(doomedInstance, 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		var done chan struct{}
		for i, inst := range instances {
			if inst.instance.ID() == uint32(id) {
				done = inst.NotifyDone()
				cancels[i]()
				cancels[i] = nil
				break
			}
		}
		if done == nil {
			t.Fatalf("Couldn't locate instance with id %d", id)
		}
		select {
		case <-done:
		case <-time.After(time.Second * 2):
			t.Fatal("Didn't get the done signal from the dead Dalga")
		}
		select {
		case <-killDone:
		case <-time.After(time.Second * 2):
			t.Fatal("Didn't finish the kill")
		}

		// Now, another instance will pick up that job and try again.
		var replacement string
		select {
		case replacement = <-killStart:
		case <-time.After(time.Second * 2):
			t.Fatal("Job wasn't picked up by a replacement instance")
		}
		if replacement == doomedInstance {
			t.Fatal("Something is very wrong")
		}
		select {
		case finished := <-killDone:
			if finished != replacement {
				t.Fatalf("Something else is very wrong, finished is '%s' but should be '%s'", finished, replacement)
			}
		case <-time.After(time.Second * 2):
			t.Fatal("Job was never finished")
		}
	})
}

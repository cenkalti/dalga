package dalga

import (
	"net/http"
	"strconv"
)

func (dalga *Dalga) makeHandler(fn func(http.ResponseWriter, *http.Request, *Dalga)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, dalga)
	}
}

// hadleSchedule is the web server endpoint for path: /schedule
func handleSchedule(w http.ResponseWriter, r *http.Request, d *Dalga) {
	routingKey, body, intervalString := r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	debug("/schedule", routingKey, body)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 32)
	if err != nil {
		http.Error(w, "Cannot parse interval", http.StatusBadRequest)
		return
	}

	if intervalUint64 < 1 {
		http.Error(w, "interval must be >= 1", http.StatusBadRequest)
		return
	}

	job := NewJob(routingKey, body, uint32(intervalUint64))
	err = job.Enter(d)
	if err != nil {
		panic(err)
	}

	// Wake up the publisher.
	//
	// publisher() may be sleeping for the next job on the queue
	// at the time we schedule a new Job. Let it wake up so it can
	// re-fetch the new Job from the front of the queue.
	//
	// The code below is an idiom for non-blocking send to a channel.
	select {
	case d.newJobs <- job:
		debug("Sent new job signal")
	default:
		debug("Did not send new job signal")
	}
}

// handleCancel is the web server endpoint for path: /cancel
func handleCancel(w http.ResponseWriter, r *http.Request, d *Dalga) {
	routingKey, body := r.FormValue("routing_key"), r.FormValue("body")
	debug("/cancel", routingKey, body)

	err := CancelJob(routingKey, body, d)
	if err != nil {
		panic(err)
	}

	select {
	case d.canceledJobs <- &Job{RoutingKey: routingKey, Body: body}:
		debug("Sent cancel signal")
	default:
		debug("Did not send cancel signal")
	}
}

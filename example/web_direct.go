package main

// This example simple runs a basic server which accepts the jobs.
// Both the server and worker run on the same instance.
// This was written quickly simple for an example, excuse the mess.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	. "github.com/gnikyt/cq"
)

// Queue instance.
var queue *Queue

// Server instance.
var srv http.Server

// Default logger.
var logr *log.Logger = log.New(os.Stdout, "", log.LstdFlags)

// Job payload.
type jobPayload struct {
	Data string `json:"data"`
}

// Job completed handler.
func jobCompleted(id int) func() {
	return func() {
		logr.Printf("[#%d] Completed unmarshal", id)
	}
}

// Job failed handler.
func jobFailed(id int) func(error) {
	return func(err error) {
		logr.Printf("[#%d] Failed unmarshal: %v", id, err)
	}
}

// Job.
func job(id int, data []byte) Job {
	retries := 3
	return WithResultHandler(
		WithRetry(func() error {
			time.Sleep(50 * time.Millisecond) // Simulate some "real work".

			// Fail every 30th job, just for an example of metrics.
			if id%30 == 0 {
				return errors.New("job: decided to fail, just because")
			}

			var pl jobPayload
			if err := json.Unmarshal(data, &pl); err != nil {
				return fmt.Errorf("job: unmarshal failed: %w", err)
			}
			return nil
		}, retries),
		jobCompleted(id),
		jobFailed(id),
	)
}

// HTTP handler.
// Send POST request with JSOn of `{"data":"[value]"}`.
func orderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusNotFound)
		return
	}

	bb, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Unable to parse body", http.StatusBadRequest)
	}

	// Enqueue job with a random ID (as a random int) and the body.
	queue.Enqueue(job(rand.Int(), bb))
}

// Metrics display.
// Open /metrics to view, auto refreshes every 500ms.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	metrics := fmt.Sprintf(`
		<table>
			<thead>
				<tr>
					<th>&nbsp;</th>
					<th>&nbsp;</th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Workers (Running)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Workers (Idle)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Jobs (Created)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Jobs (Pending)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Jobs (Active)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Jobs (Failed)</td>
					<td>%d</td>
				</tr>
				<tr>
					<td>Jobs (Completed)</td>
					<td>%d</td>
				</tr>
			</tbody>
		</table>`,
		queue.RunningWorkers(),
		queue.IdleWorkers(),
		queue.TallyOf(JobStateCreated),
		queue.TallyOf(JobStatePending),
		queue.TallyOf(JobStateActive),
		queue.TallyOf(JobStateFailed),
		queue.TallyOf(JobStateCompleted),
	)
	body := fmt.Sprintf(`
		<html>
			<head>
				<title>CG - Metrics</title>
				<meta http-equiv="refresh" content="0.5">
			</head>
			<body>%s</body>
		</html>`,
		metrics,
	)
	w.Write([]byte(body))
}

func main() {
	// Watch for cancellations.
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Create the queue and start it.
	queue = NewQueue(
		1,    // 1 workers always running.
		10,   // 10 workers max.
		1000, // 1000 jobs at a time.
		WithWorkerIdleTick(1*time.Second),
		WithContext(ctx),
	)
	queue.Start()

	fmt.Println("INFO: Send JSON data to http://localhost:8080/order")
	fmt.Println("INFO: View live metrics at http://localhost:8080/metrics")
	fmt.Println("")
	logr.Print("Queue running")

	// Create and start the server.
	srv = http.Server{Addr: ":8080"}
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/order", orderHandler)
	go func() {
		logr.Print("Server running")
		srv.ListenAndServe()
	}()

	// Listen for signal to exit.
	<-ctx.Done()

	// Stop queue.
	logr.Print("Stopping queue...")
	queue.Stop(true)
	logr.Print("Queue stopped")

	// Stop server.
	logr.Print("Stopping server...")
	srv.Shutdown(context.Background())
	logr.Print("Server stopped")
}

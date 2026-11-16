// Command simulator runs the REAL matching engine (geo-index, matcher worker
// pool, offer manager) against simulated drivers and riders, and streams
// everything that happens to a browser, so it can be drawn as a live animation.
//
//	go run ./cmd/simulator
//	open http://localhost:8080          (the animated page is built in)
//	curl -N http://localhost:8080/stream
//	curl http://localhost:8080/stats
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// The animated page is built into the program, so it works from any folder.
//
//go:embed web
var webFS embed.FS

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":8080", "address to listen on")
	flag.IntVar(&cfg.drivers, "drivers", 400, "simulated drivers online")
	flag.IntVar(&cfg.rate, "rate", 8, "ride requests per second")
	flag.IntVar(&cfg.workers, "workers", 4, "matcher worker goroutines")
	flag.IntVar(&cfg.queue, "queue", 64, "matcher queue size")
	flag.DurationVar(&cfg.offerTimeout, "offer-timeout", 8*time.Second, "how long a driver has to answer an offer")
	flag.DurationVar(&cfg.updateEvery, "update", time.Second, "time between a driver's location updates")
	flag.Float64Var(&cfg.speed, "speed", 120, "driving speed in meters per second (a time-lapse)")
	flag.BoolVar(&cfg.verbose, "verbose", false, "print the engine's logs")
	flag.StringVar(&cfg.webDir, "web", "", "serve the page from this folder instead of the built-in copy (edit it without rebuilding)")
	flag.Parse()

	if cfg.updateEvery <= 0 || cfg.speed <= 0 || cfg.workers < 1 || cfg.queue < 1 || cfg.offerTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "update, speed, workers, queue and offer-timeout must be positive")
		os.Exit(1)
	}
	if !cfg.verbose {
		log.SetOutput(io.Discard) // the offer manager logs every queue-full retry
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := newWorld(cfg)
	h := newHub()
	w.run(ctx, h)

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", w.handleStream(h))
	mux.HandleFunc("/stats", w.handleStats)
	mux.HandleFunc("/control", w.handleControl)
	if cfg.webDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(cfg.webDir)))
	} else if sub, err := fs.Sub(webFS, "web"); err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}

	srv := &http.Server{Addr: cfg.addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "server error:", err)
			stop()
		}
	}()

	fmt.Printf("simulator running on %s\n", cfg.addr)
	fmt.Printf("  drivers=%d  rate=%d/s  workers=%d  queue=%d  offer-timeout=%s\n",
		cfg.drivers, cfg.rate, cfg.workers, cfg.queue, cfg.offerTimeout)
	fmt.Println("  /stream = live frames   /stats = latest numbers   /control = change settings")
	fmt.Println("  press Ctrl+C to stop")

	<-ctx.Done()

	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	w.close()
}
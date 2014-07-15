package main

import (
	"github.com/aybabtme/color/brush"
	"github.com/aybabtme/goamz/aws"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"text/tabwriter"
	"time"
)

var (
	elog *log.Logger

	CatchSignals  = []os.Signal{os.Interrupt, os.Kill}
	SignalTimeout = time.Second * 5
	ListenAddr    = "127.0.0.1:6060" // loopback to avoid exposing it
)

func main() {

	// use all cores
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	// pretty print stdout
	w := tabwriter.NewWriter(os.Stdout, 16, 2, 2, ' ', 0)
	log.SetOutput(&lineTabWriter{w})
	log.SetFlags(log.Ltime)
	log.SetPrefix(brush.Blue("[info] ").String())

	log.Printf("starting with runtime.GOMAXPROCS=%d", numCPU)

	e, closer, err := errorLog(os.Args[0] + ".elog")
	if err != nil {
		log.Fatal(err)
	}
	elog = e
	defer func() {
		if err := closer.Close(); err != nil {
			log.Printf("ERROR: closing error logger file, %v", err)
		}
	}()

	auth, err := aws.EnvAuth()
	if err != nil {
		elog.Fatalf("need an AWS auth pair in AWS_ACCESS_KEY, AWS_SECRET_KEY: %v", err)
	}

	// long running jobs are painful to kill by mistake
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, CatchSignals...)
		log.Printf("Listening for signals: will shutdown if 2 signals received within %v.", SignalTimeout)
		for {
			elog.Printf("received signal %v: send another signal within %v to terminate", <-c, SignalTimeout)
			select {
			case <-time.After(SignalTimeout):
				elog.Printf("no signal received: continuing")
			case s := <-c:
				elog.Fatalf("received signal %v: terminating", s)
			}
		}
	}()

	go func() {
		log.Printf("Starting HTTP handler on %q, metrics and performance data available", ListenAddr)
		log.Printf("http handler failed: %v", http.ListenAndServe(ListenAddr, nil))
	}()

	if err := newApp(auth).Run(os.Args); err != nil {
		elog.Fatal(err)
	}
}

func errorLog(filename string) (*log.Logger, io.Closer, error) {
	// pretty print stderr + error file
	efile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, nil, err
	}
	out := io.MultiWriter(os.Stderr, efile)
	prfx := brush.Red("[error] ").String()
	return log.New(out, prfx, log.Ltime|log.Lshortfile), efile, nil
}

func lognotnil(err error) {
	if err != nil {
		elog.Print(err)
	}
}

// Pretty print, aligned logs.
type lineTabWriter struct {
	tab *tabwriter.Writer
}

func (l *lineTabWriter) Write(p []byte) (int, error) {
	n, err := l.tab.Write(p)
	if err != nil {
		return n, err
	}
	return n, l.tab.Flush()
}

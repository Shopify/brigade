package main

import (
	"github.com/aybabtme/color/brush"
	"github.com/crowdmob/goamz/aws"
	"io"
	"log"
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
)

func main() {
	// use all cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// pretty print stdout
	w := tabwriter.NewWriter(os.Stdout, 16, 2, 2, ' ', 0)
	log.SetOutput(&lineTabWriter{w})
	log.SetFlags(log.Ltime)
	log.SetPrefix(brush.Blue("[info] ").String())

	// pretty print stderr + error file
	efile, err := os.OpenFile(os.Args[0]+".elog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { lognotnil(efile.Close()) }()
	elog = log.New(io.MultiWriter(os.Stderr, efile), brush.Red("[error] ").String(), log.Ltime|log.Lshortfile)

	auth, err := aws.EnvAuth()
	if err != nil {
		elog.Fatalf("need an AWS auth pair in AWS_ACCESS_KEY, AWS_SECRET_KEY: %v", err)
	}

	// long running jobs are painful to kill by mistake
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, CatchSignals...)
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

	if err := newApp(auth).Run(os.Args); err != nil {
		elog.Fatal(err)
	}
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

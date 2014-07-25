package main

import (
	"github.com/Sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"time"

	// pprof attachment point
	_ "net/http/pprof"
)

var (
	catchSignals  = []os.Signal{os.Interrupt, os.Kill}
	signalTimeout = time.Second * 5
	addr          = "127.0.0.1:6060"
)

func main() {

	// use all cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// long running jobs are painful to kill by mistake
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, catchSignals...)
		for {
			logrus.WithField("signal", <-c).Warn("received signal")
			logrus.WithField("timeout", signalTimeout).Warn("send another signal to terminate")
			select {
			case <-time.After(signalTimeout):
				logrus.Info("no signal received: continuing")
			case s := <-c:
				logrus.WithField("signal", s).Fatal("terminating")
			}
		}
	}()

	// open a pprof http handler

	go func() {
		// don't monitor short tasks
		time.Sleep(time.Second * 2)
		logrus.WithFields(logrus.Fields{
			"addr":    addr,
			"metrics": "/debug/vars",
			"pprof":   "/debug/pprof",
		}).Info("monitoring handler listening")
		logrus.Fatal(http.ListenAndServe(addr, nil))
	}()

	if err := newApp().Run(os.Args); err != nil {
		logrus.WithField("error", err).Error("couldn't run app")
	}
}

func logIfErr(err error) {
	if err != nil {
		logrus.WithField("error", err).Error("unspecified error in defered call")
	}
}

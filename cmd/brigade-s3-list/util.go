package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"text/tabwriter"
)

// Helper for URL creation.
func mustURL(path string) *url.URL {
	u, err := url.Parse(path)
	if err != nil {
		log.Fatalf("%q must be a valid URL: %v", path, err)
	}
	return u
}

// flagFatal is the same as elog.Fatalf, but prints the flag usages before
// exiting the process.
func flagFatal(format string, args ...interface{}) {
	elog.Printf(format, args...)
	flag.PrintDefaults()
	os.Exit(2)
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

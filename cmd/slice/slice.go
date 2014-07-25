package slice

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/Sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"
)

// sliceTask slices a gzip'd, line separated JSON file into subfiles
type sliceTask struct{}

// subfile writes gzip'd content to its enclosing file.
type subfile struct {
	name    string
	file    *os.File
	buffer  *bufio.Writer
	gzipper *gzip.Writer
}

// newSubFile creates a new file and wraps it with a buffered,
// gzip writer.
func newSubFile(filename string) (*subfile, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("creating %q, %v", filename, err)
	}
	buffer := bufio.NewWriter(file)
	return &subfile{
		name:    filename,
		file:    file,
		buffer:  buffer,
		gzipper: gzip.NewWriter(buffer),
	}, nil
}

// Write to the subfile.
func (s *subfile) Write(p []byte) (int, error) {
	return s.gzipper.Write(p)
}

// Close the gzip stream, flush the buffer and close the file.
func (s *subfile) Close() error {
	if err := s.gzipper.Close(); err != nil {
		return fmt.Errorf("closing gzip stream for file %q", s.name)
	}

	if err := s.buffer.Flush(); err != nil {
		return fmt.Errorf("flushing buffered stream for file %q", s.name)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("closing file %q", s.name)
	}
	return nil
}

// Slice creates n subparts from the gzip'd JSON file at `filename`.
func Slice(filename string, n int) (filenames []string, err error) {
	slicer := sliceTask{}

	// open the input file
	inputfile, size, err := open(filename)
	if err != nil {
		return nil, err
	}
	defer func() { err = inputfile.Close() }()

	// create n subfiles to which we will write
	logrus.WithField("file_count", n).Info("creating output files")
	basename := filepath.Base(filename)
	subfiles := make([]*subfile, 0, n)
	// close them all, log errors but report only the last one
	defer func() {
		for _, out := range subfiles {
			if cerr := out.Close(); cerr != nil {
				logrus.WithFields(logrus.Fields{
					"error":    err,
					"filename": out.name,
				}).Error("closing output file")
				err = cerr
			}
		}
	}()
	for i := 0; i < n; i++ {
		outfilename := fmt.Sprintf("%d_%s", i, basename)
		filenames = append(filenames, outfilename)
		out, err := newSubFile(outfilename)
		if err != nil {
			return nil, fmt.Errorf("preparing subfile %d, %v", i, err)
		}
		subfiles = append(subfiles, out)
	}

	// receive buffered lines from the input file
	lines := make(chan []byte, n*2)
	doneWrite := make(chan struct{})
	start := time.Now()
	// start round-robin multiplexing of line -> subfiles, reading
	// lines from the channel
	go slicer.multiplexLines(lines, subfiles, doneWrite)

	// start feeding lines to the line multiplexer
	logrus.WithFields(logrus.Fields{
		"filename": filename,
		"size":     size,
	}).Info("reading lines from file")
	if err := readLines(inputfile, lines); err != nil {
		logrus.WithField("error", err).Error("failed to read lines from file")
	}
	logrus.WithField("duration", time.Since(start)).Info("done reading lines")

	// when all lines have been sent to subfiles, wait until they
	// finish writing
	<-doneWrite
	logrus.WithField("duration", time.Since(start)).Info("done writing to subfiles")

	return filenames, nil
}

// multipleLines receives []byte's and write them in a round-robin fashion
// to the subfiles. This is not optimal since writes to disk are not sequential
// but it's simpler to reason about, and performance doesn't really matter.
func (st *sliceTask) multiplexLines(lines <-chan []byte, outputs []*subfile, done chan<- struct{}) {
	defer close(done)
	outIdx := 0
	outMod := len(outputs)
	for line := range lines {
		// write the lines in a round robin way
		_, err := outputs[outIdx].Write(line)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error":       err,
				"output_name": outputs[outIdx].name,
			}).Error("couldn't write to output")
			return
		}
		outIdx = (outIdx + 1) % outMod
	}
}

// readLines from r, which contains gzip'd data in a line (\n) oriented
// way. Each line of []byte is sent to the output `lines` channel
func readLines(r io.Reader, lines chan<- []byte) error {
	defer close(lines)

	// gunzip the reader (proxied by the progress bar)
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer func() { _ = gr.Close() }()

	rd := bufio.NewReader(gr)

	var total uint64
	defer func() { logrus.WithField("decompressed_size", total).Info("done decompressing") }()

	// reads all the `\n` separated lines until EOF
	for {
		line, err := rd.ReadBytes('\n')
		switch err {
		case nil:
			// no error, carry on
		case io.EOF:
			// done reading
			return nil
		default:
			// not nil, not EOF, bail
			return err
		}
		total += uint64(len(line) + 1)
		lines <- line
	}
}

// open a file and stat its size (in bytes).
func open(filename string) (*os.File, int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, -1, fmt.Errorf("opening file %q: %v", filename, err)
	}
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, -1, fmt.Errorf("stating file %q: %v", filename, err)
	}
	return file, fi.Size(), nil
}

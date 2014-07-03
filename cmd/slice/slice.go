package slice

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// sliceTask slices a gzip'd, line separated JSON file into subfiles
type sliceTask struct {
	elog *log.Logger
}

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
func Slice(el *log.Logger, filename string, n int) (filenames []string, err error) {
	slicer := sliceTask{elog: el}

	// open the input file
	inputfile, size, err := open(filename)
	if err != nil {
		return nil, err
	}
	defer func() { err = inputfile.Close() }()

	// create n subfiles to which we will write
	log.Printf("creating %d output files", n)
	basename := filepath.Base(filename)
	subfiles := make([]*subfile, 0, n)
	// close them all, log errors but report only the last one
	defer func() {
		for _, out := range subfiles {
			if cerr := out.Close(); cerr != nil {
				el.Print(err)
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
	log.Printf("reading lines from %q (%s)", filename, humanize.Bytes(uint64(size)))
	if err := readLines(inputfile, size, lines); err != nil {
		el.Printf("reading lines from input failed: %v", err)
	}
	log.Printf("done reading lines in %v", time.Since(start))

	// when all lines have been sent to subfiles, wait until they
	// finish writing
	<-doneWrite
	log.Printf("done writing to subfiles in %v", time.Since(start))

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
			st.elog.Printf("couldn't write to output %d: %v", outIdx, err)
			return
		}
		outIdx = (outIdx + 1) % outMod
	}
}

// readLines from r, which contains gzip'd data in a line (\n) oriented
// way. Each line of []byte is sent to the output `lines` channel
// A progress bar is printed to stdout as the input file is read.
func readLines(r io.Reader, size int64, lines chan<- []byte) error {
	defer close(lines)
	// wraps the reader with a progress bar, because it's convenient
	// to know what's going on while the tool runs.
	bar := pb.New64(size)
	bar.ShowSpeed = true
	bar.SetUnits(pb.U_BYTES)
	barr := bar.NewProxyReader(r)

	// gunzip the reader (proxied by the progress bar)
	gr, err := gzip.NewReader(barr)
	if err != nil {
		return err
	}
	defer func() { _ = gr.Close() }()

	rd := bufio.NewReader(gr)

	bar.Start()
	defer bar.FinishPrint("all lines were read")
	var total uint64
	defer func() { log.Printf("total decompressed size %s", humanize.Bytes(total)) }()

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

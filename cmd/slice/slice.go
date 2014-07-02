package slice

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/bradfitz/iter"
	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type sliceTask struct {
	elog *log.Logger
}

type subfile struct {
	name    string
	file    *os.File
	buffer  *bufio.Writer
	gzipper *gzip.Writer
}

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

func (s *subfile) Write(p []byte) (int, error) {
	return s.gzipper.Write(p)
}

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

	inputfile, size, err := open(el, filename)
	if err != nil {
		return nil, err
	}
	defer func() { err = inputfile.Close() }()

	log.Printf("creating %d output files", n)
	basename := filepath.Base(filename)

	subfiles := make([]*subfile, 0, n)
	defer func() {
		for _, out := range subfiles {
			if cerr := out.Close(); cerr != nil {
				el.Print(err)
				err = cerr
			}
		}
	}()

	for i := range iter.N(n) {
		outfilename := fmt.Sprintf("%d_%s", i, basename)
		filenames = append(filenames, outfilename)
		out, err := newSubFile(outfilename)
		if err != nil {
			return nil, fmt.Errorf("preparing subfile %d, %v", i, err)
		}
		subfiles = append(subfiles, out)
	}

	lines := make(chan []byte, n*2)
	doneWrite := make(chan struct{})
	start := time.Now()
	go slicer.multiplexLines(lines, subfiles, doneWrite)

	log.Printf("reading lines from %q (%s)", filename, humanize.Bytes(uint64(size)))
	if err := readLines(inputfile, size, lines); err != nil {
		el.Printf("reading lines from input failed: %v", err)
	}
	log.Printf("done reading lines in %v", time.Since(start))

	<-doneWrite
	log.Printf("done writing to subfiles in %v", time.Since(start))

	return filenames, nil
}

func (st *sliceTask) multiplexLines(lines <-chan []byte, outputs []*subfile, done chan<- struct{}) {
	defer close(done)
	outIdx := 0
	outMod := len(outputs)
	for line := range lines {
		_, err := outputs[outIdx].Write(line)
		if err != nil {
			st.elog.Printf("couldn't write to output %d: %v", outIdx, err)
			return
		}
		outIdx = (outIdx + 1) % outMod
	}
}

func readLines(r io.Reader, size int64, lines chan<- []byte) error {
	defer close(lines)
	bar := pb.New(int(size))
	bar.ShowBar = true
	bar.ShowCounters = true
	bar.ShowPercent = true
	bar.ShowSpeed = true
	bar.ShowTimeLeft = true
	bar.SetUnits(pb.U_BYTES)
	barr := bar.NewProxyReader(r)

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
	for {
		line, err := rd.ReadBytes('\n')
		switch err {
		case io.EOF:
			return nil
		case nil:
		default:
			return err
		}
		total += uint64(len(line) + 1)
		lines <- line
	}
}

func open(elog *log.Logger, filename string) (*os.File, int64, error) {
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

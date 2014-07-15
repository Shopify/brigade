package main

import (
	"compress/gzip"
	"fmt"
	"github.com/Shopify/brigade/cmd/diff"
	"github.com/Shopify/brigade/cmd/list"
	"github.com/Shopify/brigade/cmd/slice"
	"github.com/Shopify/brigade/cmd/sync"
	"github.com/aybabtme/goamz/aws"
	"github.com/aybabtme/goamz/s3"
	"github.com/cheggaaa/pb"
	"github.com/codegangsta/cli"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
)

var version = "0.0.1"

// Those are set by the `GOLDFLAGS` in the Makefile.
var branch, commit string

func newApp(auth aws.Auth) *cli.App {
	app := cli.NewApp()
	app.Name = "brigade"
	app.Usage = "Toolkit to list and sync S3 buckets."
	app.Version = fmt.Sprintf("%s (%s, %s)", version, branch, commit)

	app.Commands = []cli.Command{
		listCommand(auth),
		syncCommand(auth),
		sliceCommand(),
		diffCommand(),
	}

	return app
}

func listCommand(auth aws.Auth) cli.Command {

	var (
		bucketFlag = cli.StringFlag{Name: "bucket", Value: "", Usage: "path to bucket to list, of the form s3://name/path/"}
		dstFlag    = cli.StringFlag{Name: "dest", Value: "bucket_list.json.gz", Usage: "filename to which the list of keys is saved"}
		regionFlag = cli.StringFlag{Name: "aws-region", Value: aws.USEast.Name, Usage: "AWS region where the bucket lives"}
		dedupFlag  = cli.BoolFlag{Name: "dedup", Usage: "deduplicate jobs and keys, consumes much more memory"}
	)

	return cli.Command{
		Name:  "list",
		Usage: "Lists the keys in an S3 bucket.",
		Description: strings.TrimSpace(`
Do a traversal of the S3 bucket using many concurrent workers. The result of
traversing is saved and gzip'd as a list of s3 keys in JSON form.`),
		Flags: []cli.Flag{bucketFlag, dstFlag, regionFlag, dedupFlag},
		Action: func(c *cli.Context) {

			bucket := c.String(bucketFlag.Name)
			dest := c.String(dstFlag.Name)
			regionName := c.String(regionFlag.Name)
			dedup := c.Bool(dedupFlag.Name)

			region, validRegion := aws.Regions[regionName]

			hadError := true

			file, dsterr := os.Create(dest)
			if dsterr == nil {
				defer func() { logIfErr(file.Close()) }()
			}

			switch {
			case bucket == "":
				elog.Printf("invalid bucket name: %q", bucket)
			case !validRegion:
				elog.Printf("invalid aws-region: %q", regionName)
			case dsterr != nil:
				elog.Printf("couldn't create %q: %v", dest, dsterr)
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			gw := gzip.NewWriter(file)
			defer func() { logIfErr(gw.Close()) }()

			log.Printf("starting command %q", c.Command.Name)
			log.Printf("args=%v", os.Args)

			err := list.List(elog, s3.New(auth, region), bucket, gw, dedup)
			if err != nil {
				elog.Printf("failed to list bucket: %v", err)
			}
		},
	}
}

func syncCommand(auth aws.Auth) cli.Command {
	var (
		inputFlag       = cli.StringFlag{Name: "input", Usage: "name of the file containing the list of keys to sync"}
		successFlag     = cli.StringFlag{Name: "success", Usage: "name of the output file where to write the list of keys that succeeded to sync, defaults to /dev/null"}
		failureFlag     = cli.StringFlag{Name: "failure", Usage: "name of the output file where to write the list of keys that failed to sync, defaults to /dev/null"}
		srcFlag         = cli.StringFlag{Name: "src", Usage: "source bucket to get the keys from"}
		dstFlag         = cli.StringFlag{Name: "dest", Usage: "destination bucket to put the keys into"}
		regionFlag      = cli.StringFlag{Name: "aws-region", Value: aws.USEast.Name, Usage: "AWS region where the buckets lives"}
		concurrencyFlag = cli.IntFlag{Name: "concurrency", Value: 200, Usage: "number of concurrent sync request"}
	)

	return cli.Command{
		Name:  "sync",
		Usage: "Syncs the keys from a source S3 bucket to another.",
		Description: strings.TrimSpace(`
Reads the keys from an s3 key listing and sync them one by one from a source
bucket to a destination bucket.`),
		Flags: []cli.Flag{inputFlag, successFlag, failureFlag, srcFlag, dstFlag, regionFlag, concurrencyFlag},
		Action: func(c *cli.Context) {

			inputFilename := c.String(inputFlag.Name)
			successFilename := c.String(successFlag.Name)
			failureFilename := c.String(failureFlag.Name)
			src := c.String(srcFlag.Name)
			dest := c.String(dstFlag.Name)
			regionName := c.String(regionFlag.Name)
			conc := c.Int(concurrencyFlag.Name)

			srcU, srcErr := url.Parse(src)
			dstU, dstErr := url.Parse(dest)
			region, validRegion := aws.Regions[regionName]
			hadError := true
			switch {
			case !validRegion:
				elog.Printf("%q is not a valid region name", regionName)
			case src == "":
				elog.Printf("need a source bucket to sync from")
			case srcErr != nil:
				elog.Printf("%q is not a valid source URL: %v", src, srcErr)
			case dest == "":
				elog.Printf("need a destination bucket to sync onto")
			case dstErr != nil:
				elog.Printf("%q is not a valid source URL: %v", dest, dstErr)
			case inputFilename == "":
				elog.Printf("need an input file to read keys from")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			listfile, err := os.Open(inputFilename)
			if err != nil {
				elog.Printf("couldn't open listing file: %v", err)
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}
			defer func() { logIfErr(listfile.Close()) }()

			fi, err := listfile.Stat()
			if err != nil {
				elog.Fatalf("couldn't stat listing file: %v", err)
			}

			createOutput := func(filename string) (io.Writer, func() error, error) {
				if filename == "" {
					file, err := os.Open(os.DevNull)
					closer := func() error { return nil }
					return file, closer, err
				}

				file, err := os.Create(filename)
				if err != nil {
					return nil, nil, err
				}
				gzFile := gzip.NewWriter(file)
				closer := func() error {
					if err := gzFile.Close(); err != nil {
						elog.Printf("closing gzip writer to %q: %v", filename, err)
					}
					return file.Close()
				}
				return gzFile, closer, nil
			}

			successFile, sucCloser, err := createOutput(successFilename)
			if err != nil {
				elog.Fatalf("couldn't create success key file: %v", err)
			}
			defer func() { logIfErr(sucCloser()) }()

			failureFile, failCloser, err := createOutput(failureFilename)
			if err != nil {
				elog.Fatalf("couldn't create failure key file: %v", err)
			}
			defer func() { logIfErr(failCloser()) }()

			successGzWr := gzip.NewWriter(successFile)
			defer func() { logIfErr(successGzWr.Close()) }()
			failureGzWr := gzip.NewWriter(failureFile)
			defer func() { logIfErr(failureGzWr.Close()) }()

			// tracking the progress in reading the file helps tracking
			// how far in the sync process we are.
			bar := pb.New64(fi.Size())
			bar.ShowSpeed = true
			bar.SetUnits(pb.U_BYTES)
			barr := bar.NewProxyReader(listfile)

			inputGzRd, err := gzip.NewReader(barr)
			if err != nil {
				elog.Printf("listing file is not a gzip file: %v", err)
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}
			defer func() { logIfErr(inputGzRd.Close()) }()

			bar.Start()
			defer bar.Finish()

			sss := s3.New(auth, region)
			srcBkt := sss.Bucket(srcU.Host)
			dstBkt := sss.Bucket(dstU.Host)

			log.Printf("starting command %q", c.Command.Name)
			log.Printf("args=%v", os.Args)

			syncTask, err := sync.NewSyncTask(elog, srcBkt, dstBkt)
			if err != nil {
				elog.Printf("failed to prepare sync task, %v", err)
				return
			}
			syncTask.SyncPara = conc
			err = syncTask.Start(inputGzRd, successGzWr, failureGzWr)
			if err != nil {
				elog.Printf("failed to sync: %v", err)
			}
		},
	}
}

func sliceCommand() cli.Command {
	var (
		nFlag        = cli.IntFlag{Name: "n", Value: 0, Usage: "number of slices to split the S3 key listing over"}
		filenameFlag = cli.StringFlag{Name: "src", Value: "", Usage: "file from which to read the S3 key listing"}
	)

	return cli.Command{
		Name:  "slice",
		Usage: "Slice an S3 key listing into multiple sub-listings.",
		Description: strings.TrimSpace(`
Slices a listing of S3 keys into multiple files, each containing evenly
distributed keys. It expects a key listing in the form of a gzip'd JSON file
and will produce such files in return. Each file is prefixed by its index,
so calling:
	brigade slice -n 3 -src bucket.json.gz
Will produce the files:
	0_bucket.json.gz
	1_bucket.json.gz
	2_bucket.json.gz`),
		Flags: []cli.Flag{nFlag, filenameFlag},
		Action: func(c *cli.Context) {

			n := c.Int(nFlag.Name)
			filename := c.String(filenameFlag.Name)

			hadError := true
			switch {
			case filename == "":
				elog.Printf("need a file to slice")
			case n <= 1:
				elog.Printf("need to slice in at least 2 parts")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			log.Printf("starting command %q", c.Command.Name)
			log.Printf("args=%v", os.Args)

			_, err := slice.Slice(elog, filename, n)
			if err != nil {
				elog.Printf("failed to slice: %v", err)
			}

		},
	}
}

func diffCommand() cli.Command {
	var (
		oldfileFlag = cli.StringFlag{Name: "old", Usage: "old file from which to read s3 keys"}
		newfileFlag = cli.StringFlag{Name: "new", Usage: "new file from which to read s3 keys"}
		dstfileFlag = cli.StringFlag{Name: "dest", Usage: "destination file where to write the keys that have changed"}
	)

	return cli.Command{
		Name:  "diff",
		Usage: "Generates a differential listing of S3 keys.",
		Description: strings.TrimSpace(`
Reads from an old s3 key listing and a new one, computes which keys have changed
in the new listing and generates a new files containing only those keys.`),
		Flags: []cli.Flag{oldfileFlag, newfileFlag, dstfileFlag},
		Action: func(c *cli.Context) {

			oldfile := c.String(oldfileFlag.Name)
			newfile := c.String(newfileFlag.Name)
			dstfile := c.String(dstfileFlag.Name)

			hadError := true
			switch {
			case oldfile == "":
				elog.Print("need a filename for old key listing")
			case newfile == "":
				elog.Print("need a filename for new key listing")
			case dstfile == "":
				elog.Print("need a filename for dest key listing")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			open := func(filename string) *os.File {
				f, err := os.Open(filename)
				if err != nil {
					elog.Fatalf("couldn't open file %q: %v", filename, err)
				}
				return f
			}

			newf := open(newfile)
			defer func() { logIfErr(newf.Close()) }()
			oldf := open(oldfile)
			defer func() { logIfErr(oldf.Close()) }()

			dstf, err := os.Create(dstfile)
			if err != nil {
				elog.Fatalf("couldn't create destination file %q: %v", dstfile, err)
			}
			defer func() { logIfErr(dstf.Close()) }()

			gzread := func(f *os.File) *gzip.Reader {
				gzr, err := gzip.NewReader(f)
				if err != nil {
					elog.Fatalf("couldn't read gzip from %q: %v", f.Name(), err)
				}
				return gzr
			}

			newgz := gzread(newf)
			oldgz := gzread(oldf)
			dstgz := gzip.NewWriter(dstf)
			defer func() { logIfErr(dstgz.Close()) }()

			log.Printf("starting command %q", c.Command.Name)
			log.Printf("args=%v", os.Args)

			if err := diff.Diff(elog, oldgz, newgz, dstgz); err != nil {
				elog.Printf("failed to diff: %v", err)
			}
		},
	}
}

package main

import (
	"compress/gzip"
	"fmt"
	"github.com/Shopify/brigade/cmd/diff"
	"github.com/Shopify/brigade/cmd/list"
	"github.com/Shopify/brigade/cmd/slice"
	"github.com/Shopify/brigade/cmd/sync"
	"github.com/Sirupsen/logrus"
	"github.com/aybabtme/goamz/aws"
	"github.com/aybabtme/goamz/s3"
	"github.com/codegangsta/cli"
	"io"
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
				logrus.WithField("name", bucket).Error("invalid bucket name")
			case !validRegion:
				logrus.WithField("region", regionName).Error("invalid aws-region")
			case dsterr != nil:
				logrus.WithFields(logrus.Fields{
					"error":    dsterr,
					"filename": dest,
				}).Error("couldn't create destination file")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			gw := gzip.NewWriter(file)
			defer func() { logIfErr(gw.Close()) }()

			logrus.Info("starting command", c.Command.Name)

			err := list.List(s3.New(auth, region), bucket, gw, dedup)
			if err != nil {
				logrus.WithField("error", err).Error("failed to list bucket")
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
				logrus.WithField("region", regionName).Error("invalid aws-region")
			case src == "":
				logrus.Error("need a source bucket to sync from")
			case srcErr != nil:
				logrus.WithFields(logrus.Fields{
					"error":  srcErr,
					"source": src,
				}).Error("not a valid source URL")
			case dest == "":
				logrus.Error("need a destination bucket to sync onto")
			case dstErr != nil:
				logrus.WithFields(logrus.Fields{
					"error":       srcErr,
					"destination": dest,
				}).Error("not a valid destination URL")
			case inputFilename == "":
				logrus.Error("need an input file to read keys from")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			listfile, err := os.Open(inputFilename)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error":    err,
					"filename": inputFilename,
				}).Error("couldn't open listing file")
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}
			defer func() { logIfErr(listfile.Close()) }()

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
						logrus.WithFields(logrus.Fields{
							"error":    err,
							"filename": filename,
						}).Error("closing gzip writer")
					}
					return file.Close()
				}
				return gzFile, closer, nil
			}

			successFile, sucCloser, err := createOutput(successFilename)
			if err != nil {
				logrus.WithField("error", err).Error("couldn't create success key file")
			}
			defer func() { logIfErr(sucCloser()) }()

			failureFile, failCloser, err := createOutput(failureFilename)
			if err != nil {
				logrus.WithField("error", err).Error("couldn't create failure key file")
			}
			defer func() { logIfErr(failCloser()) }()

			inputGzRd, err := gzip.NewReader(listfile)
			if err != nil {
				logrus.WithField("error", err).Error("listing file is not a gzip file")
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}
			defer func() { logIfErr(inputGzRd.Close()) }()

			sss := s3.New(auth, region)
			srcBkt := sss.Bucket(srcU.Host)
			dstBkt := sss.Bucket(dstU.Host)

			logrus.Info("starting command", c.Command.Name)

			syncTask, err := sync.NewSyncTask(srcBkt, dstBkt)
			if err != nil {
				logrus.WithField("error", err).Error("failed to prepare sync task")
				return
			}
			syncTask.SyncPara = conc
			err = syncTask.Start(inputGzRd, successFile, failureFile)
			if err != nil {
				logrus.WithField("error", err).Error("failed to sync")
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
				logrus.Error("need a file to slice")
			case n <= 1:
				logrus.Error("need to slice in at least 2 parts")
			default:
				hadError = false
			}
			if hadError {
				cli.ShowCommandHelp(c, c.Command.Name)
				return
			}

			logrus.Info("starting command", c.Command.Name)

			_, err := slice.Slice(filename, n)
			if err != nil {
				logrus.WithField("error", err).Error("failed to slice")
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
				logrus.Error("need a filename for old key listing")
			case newfile == "":
				logrus.Error("need a filename for new key listing")
			case dstfile == "":
				logrus.Error("need a filename for dest key listing")
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
					logrus.WithFields(logrus.Fields{
						"error":    err,
						"filename": filename,
					}).Fatal("couldn't open file")
				}
				return f
			}

			newf := open(newfile)
			defer func() { logIfErr(newf.Close()) }()
			oldf := open(oldfile)
			defer func() { logIfErr(oldf.Close()) }()

			dstf, err := os.Create(dstfile)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error":    err,
					"filename": dstfile,
				}).Fatal("couldn't create destination file")
			}
			defer func() { logIfErr(dstf.Close()) }()

			gzread := func(f *os.File) *gzip.Reader {
				gzr, err := gzip.NewReader(f)
				if err != nil {
					logrus.WithFields(logrus.Fields{
						"error":    err,
						"filename": f.Name(),
					}).Fatal("couldn't read gzip")
				}
				return gzr
			}

			newgz := gzread(newf)
			oldgz := gzread(oldf)
			dstgz := gzip.NewWriter(dstf)
			defer func() { logIfErr(dstgz.Close()) }()

			logrus.Info("starting command", c.Command.Name)

			if err := diff.Diff(oldgz, newgz, dstgz); err != nil {
				logrus.WithField("error", err).Error("failed to diff")
			}
		},
	}
}

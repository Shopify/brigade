package main

import (
	"compress/gzip"
	"fmt"
	"github.com/Shopify/brigade/cmd/backup"
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

func newApp() *cli.App {
	app := cli.NewApp()
	app.Name = "brigade"
	app.Usage = "Toolkit to list and sync S3 buckets."
	app.Version = fmt.Sprintf("%s (%s, %s)", version, branch, commit)

	app.Commands = []cli.Command{
		listCommand(),
		syncCommand(),
		sliceCommand(),
		diffCommand(),
		backupCommand(),
	}

	return app
}

func mustURL(c *cli.Context, f cli.StringFlag) *url.URL {
	s := mustString(c, f)
	u, err := url.Parse(s)
	if err != nil {
		cli.ShowCommandHelp(c, c.Command.Name)
		logrus.WithField("url", s).Fatal("not a valid url")
	}
	return u
}

func mustRegion(c *cli.Context, f cli.StringFlag) aws.Region {
	s := mustString(c, f)
	r, ok := aws.Regions[s]
	if !ok {
		var valids []string
		for key := range aws.Regions {
			valids = append(valids, key)
		}

		cli.ShowCommandHelp(c, c.Command.Name)
		logrus.WithFields(logrus.Fields{
			"url":           s,
			"valid_regions": valids,
		}).Fatal("not a valid AWS region")
	}
	return r
}

func mustString(c *cli.Context, f cli.StringFlag) string {
	s := c.String(f.Name)
	if s == "" && f.Value == "" {
		cli.ShowCommandHelp(c, c.Command.Name)
		logrus.WithField("flag", f.Name).Fatal("flag is mandatory")
	}
	return s
}

func listCommand() cli.Command {

	var (
		accessFlag = cli.StringFlag{Name: "access", Usage: "AWS access key to source bucket"}
		secretFlag = cli.StringFlag{Name: "secret", Usage: "AWS secret key to source bucket"}
		regionFlag = cli.StringFlag{Name: "region", Usage: "region of source bucket to get the keys from"}

		bucketFlag = cli.StringFlag{Name: "bucket", Value: "", Usage: "path to bucket to list, of the form s3://name/path/"}
		destFlag   = cli.StringFlag{Name: "dest", Value: "bucket_list.json.gz", Usage: "filename to which the list of keys is saved"}
	)

	return cli.Command{
		Name:  "list",
		Usage: "Lists the keys in an S3 bucket.",
		Description: strings.TrimSpace(`
Do a traversal of the S3 bucket using many concurrent workers. The result of
traversing is saved and gzip'd as a list of s3 keys in JSON form.`),
		Flags: []cli.Flag{
			accessFlag,
			secretFlag,
			bucketFlag,
			destFlag,
			regionFlag,
		},
		Action: func(c *cli.Context) {

			bkt := mustURL(c, bucketFlag)
			dest := mustString(c, destFlag)
			srcS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, accessFlag),
				SecretKey: mustString(c, secretFlag),
			}, mustRegion(c, regionFlag))

			file, dsterr := os.Create(dest)
			if dsterr != nil {
				cli.ShowCommandHelp(c, c.Command.Name)
				logrus.WithFields(logrus.Fields{
					"error":    dsterr,
					"filename": dest,
				}).Error("couldn't create destination file")
				return
			}
			defer func() { logIfErr(file.Close()) }()

			gw := gzip.NewWriter(file)
			defer func() { logIfErr(gw.Close()) }()

			logrus.Info("starting command ", c.Command.Name)

			err := list.List(srcS3, bkt.Host, bkt.Path, gw)
			if err != nil {
				logrus.WithField("error", err).Error("failed to list bucket")
			}
		},
	}
}

func syncCommand() cli.Command {
	var (
		srcAccess = cli.StringFlag{Name: "src-access", Usage: "AWS access key to source bucket"}
		srcSecret = cli.StringFlag{Name: "src-secret", Usage: "AWS secret key to source bucket"}
		srcRegion = cli.StringFlag{Name: "src-region", Usage: "region of source bucket to get the keys from"}

		destAccess = cli.StringFlag{Name: "dest-access", Usage: "AWS access key to destination bucket"}
		destSecret = cli.StringFlag{Name: "dest-secret", Usage: "AWS secret key to destination bucket"}
		destRegion = cli.StringFlag{Name: "dest-region", Usage: "region of destination bucket to write the keys to"}

		inputFlag       = cli.StringFlag{Name: "input", Usage: "name of the file containing the list of keys to sync"}
		successFlag     = cli.StringFlag{Name: "success", Usage: "name of the output file where to write the list of keys that succeeded to sync, defaults to /dev/null"}
		failureFlag     = cli.StringFlag{Name: "failure", Usage: "name of the output file where to write the list of keys that failed to sync, defaults to /dev/null"}
		srcFlag         = cli.StringFlag{Name: "src", Usage: "source bucket to get the keys from"}
		dstFlag         = cli.StringFlag{Name: "dest", Usage: "destination bucket to put the keys into"}
		concurrencyFlag = cli.IntFlag{Name: "concurrency", Value: 200, Usage: "number of concurrent sync request"}
	)

	return cli.Command{
		Name:  "sync",
		Usage: "Syncs the keys from a source S3 bucket to another.",
		Description: strings.TrimSpace(`
Reads the keys from an s3 key listing and sync them one by one from a source
bucket to a destination bucket.`),
		Flags: []cli.Flag{
			srcAccess,
			srcSecret,
			srcRegion,
			destAccess,
			destSecret,
			destRegion,
			inputFlag,
			successFlag,
			failureFlag,
			srcFlag,
			dstFlag,
			concurrencyFlag,
		},
		Action: func(c *cli.Context) {

			inputFilename := mustString(c, inputFlag)
			successFilename := mustString(c, successFlag)
			failureFilename := mustString(c, failureFlag)
			src := mustURL(c, srcFlag)
			dest := mustURL(c, dstFlag)
			conc := c.Int(concurrencyFlag.Name)

			srcS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, srcAccess),
				SecretKey: mustString(c, srcSecret),
			}, mustRegion(c, srcRegion))
			srcBkt := srcS3.Bucket(src.Host)

			destS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, destAccess),
				SecretKey: mustString(c, destSecret),
			}, mustRegion(c, destRegion))
			destBkt := destS3.Bucket(dest.Host)

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

			logrus.Info("starting command ", c.Command.Name)

			syncTask, err := sync.NewSyncTask(srcBkt, destBkt)
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

			logrus.Info("starting command ", c.Command.Name)

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

			logrus.Info("starting command ", c.Command.Name)

			if err := diff.Diff(oldgz, newgz, dstgz); err != nil {
				logrus.WithField("error", err).Error("failed to diff")
			}
		},
	}
}

func backupCommand() cli.Command {
	var (
		srcFlag   = cli.StringFlag{Name: "src", Usage: "source bucket to get the keys from"}
		destFlag  = cli.StringFlag{Name: "dest", Usage: "destination bucket to put the keys into"}
		stateFlag = cli.StringFlag{Name: "state", Usage: "state bucket where artifacts of backups are held"}

		srcAccess = cli.StringFlag{Name: "src-access", Usage: "AWS access key to source bucket"}
		srcSecret = cli.StringFlag{Name: "src-secret", Usage: "AWS secret key to source bucket"}
		srcRegion = cli.StringFlag{Name: "src-region", Usage: "region of source bucket to get the keys from"}

		destAccess = cli.StringFlag{Name: "dest-access", Usage: "AWS access key to destination bucket"}
		destSecret = cli.StringFlag{Name: "dest-secret", Usage: "AWS secret key to destination bucket"}
		destRegion = cli.StringFlag{Name: "dest-region", Usage: "region of destination bucket to write the keys to"}

		stateAccess = cli.StringFlag{Name: "state-access", Usage: "AWS access key to state bucket"}
		stateSecret = cli.StringFlag{Name: "state-secret", Usage: "AWS secret key to state bucket"}
		stateRegion = cli.StringFlag{Name: "state-region", Usage: "region of state bucket to persist artifacts into"}
	)

	return cli.Command{
		Name:  "backup",
		Usage: "Executes list, diff and sync from a source to a destination bucket.",
		Description: strings.TrimSpace(`
Executes list, diff and sync one after another. It works against a
'state' s3 bucket, which contains past backups and where this backup
will store its output.`),
		Flags: []cli.Flag{
			srcFlag,
			destFlag,
			stateFlag,
			srcAccess,
			srcSecret,
			srcRegion,
			destAccess,
			destSecret,
			destRegion,
			stateAccess,
			stateSecret,
			stateRegion,
		},
		Action: func(c *cli.Context) {

			src := mustURL(c, srcFlag)
			dest := mustURL(c, destFlag)
			state := mustURL(c, stateFlag)

			srcS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, srcAccess),
				SecretKey: mustString(c, srcSecret),
			}, mustRegion(c, srcRegion))
			srcBkt := srcS3.Bucket(src.Host)

			destS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, destAccess),
				SecretKey: mustString(c, destSecret),
			}, mustRegion(c, destRegion))
			destBkt := destS3.Bucket(dest.Host)

			stateS3 := s3.New(aws.Auth{
				AccessKey: mustString(c, stateAccess),
				SecretKey: mustString(c, stateSecret),
			}, mustRegion(c, stateRegion))
			stateBkt := stateS3.Bucket(state.Host)

			logrus.Info("starting command ", c.Command.Name)

			srcPath := src.Path
			if strings.HasPrefix(srcPath, "/") {
				srcPath = srcPath[1:]
			}

			statePath := state.Path
			if strings.HasPrefix(statePath, "/") {
				statePath = statePath[1:]
			}

			task, err := backup.NewBackup(srcBkt, destBkt, stateBkt, srcPath, statePath)
			if err != nil {
				logrus.WithField("error", err).Error("failed to prepare backup task")
				return
			}
			defer func() {
				if err := task.Cleanup(); err != nil {
					logrus.WithField("error", err).Error("failed to close backup task")
				}
			}()

			if err := task.Execute(); err != nil {
				logrus.WithField("error", err).Error("failed to backup")
			}

		},
	}
}

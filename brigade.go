package brigade

import (
	"container/list"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
)

var dirQueue *list.List
var fileQueue chan string

type S3Connection struct {
	Source      *s3.S3
	Destination *s3.S3
}

func S3Connect(t *Target) *s3.S3 {
	auth := aws.Auth{t.AccessKey, t.SecretAccessKey}
	return s3.New(auth, aws.Region{S3Endpoint: t.Server})
}

func S3Init() *S3Connection {
	s := &S3Connection{S3Connect(Config.Source), S3Connect(Config.Destination)}

	if s.Source == nil {
		log.Fatalf("Could not connect to S3 endpoint %s", Config.Source.Server)
	}

	if s.Destination == nil {
		log.Fatalf("Could not connect to S3 endpoint %s", Config.Destination.Server)
	}

	return s
}

func (s *S3Connection) fileWorker() {
	// pull files off channel, copy with permissions
}

func CopyBucket() {
	dirQueue = list.New()

	// spawn workers
	for i := 0; i < Config.Workers; i++ {
		go fileCopier()
	}

}

func copyDirectory(dir string) {
	// get list on source and destination

	// push subdirectories onto directory queue

	// push changed files onto file queue

}

func fileCopier() {
}

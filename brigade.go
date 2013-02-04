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
	Connection *s3.S3
	Target     *Target
}

func S3Connect(target *Target) *S3Connection {
	s := &S3Connection{nil, target}
	auth := aws.Auth{s.Target.AccessKey, s.Target.SecretAccessKey}
	s.Connection = s3.New(auth, aws.Region{S3Endpoint: s.Target.Server})

	if s.Connection == nil {
		log.Fatalf("Could not connect to S3 endpoint %s", s.Target.Server)
	}
	return s
}

func (s *S3Connection) fileWorker() {
	// pull files off channel, copy with permissions
}

func CopyBucket() {
	dirQueue = list.New()

	// spawn workers
	for i := 0; i < env.Workers; i++ {
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

package brigade

import (
	"launchpad.net/goamz/s3"
	"os"
	"testing"
)

var sourceBucketName string = "brigade-test-source"
var destBucketName string = "brigade-test-destination"

func TestCredentials(t *testing.T) {
	if os.Getenv("ACCESS_KEY") == "" || os.Getenv("SECRET_ACCESS_KEY") == "" || os.Getenv("AWS_HOST") == "" {
		t.Error("Please set ACCESS_KEY, SECRET_ACCESS_KEY, and AWS_HOST variables for integration tests")
	}
}

func LoadTarget(bucket string) *Target {
	return &Target{os.Getenv("AWS_HOST"), bucket, os.Getenv("ACCESS_KEY"), os.Getenv("SECRET_ACCESS_KEY")}
}

type fileFixture struct {
	key  string
	data []byte
	mime string
	perm s3.ACL
}

var fixtures []fileFixture = []fileFixture{
	{"house", []byte("house data"), "text/plain", s3.PublicRead},
	{"house2", []byte("house2 data"), "text/plain", s3.PublicRead},
	{"animals/cat", []byte("first cat"), "text/plain", s3.PublicRead},
	{"animals/dog", []byte("second cat"), "text/plain", s3.PublicRead}}

func SetupBuckets() error {
	source := S3Connect(LoadTarget(sourceBucketName))
	dest := S3Connect(LoadTarget(destBucketName))

	sourceBucket := source.Bucket(sourceBucketName)
	destBucket := dest.Bucket(destBucketName)

	err := sourceBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	err = destBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	for i := 0; i < len(fixtures); i++ {
		err = sourceBucket.Put(fixtures[i].key, fixtures[i].data, fixtures[i].mime, fixtures[i].perm)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestConnection(t *testing.T) {
	conn := S3Connect(LoadTarget(sourceBucketName))

	if conn == nil {
		t.Error("Could not connect to S3 host.  Check network & credentials")
	}
}

func TestCopyDirectory(t *testing.T) {
	err := SetupBuckets()

	if err != nil {
		t.Error("Failed to set up buckets")
	}
}

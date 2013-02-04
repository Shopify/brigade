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

func SetupBuckets() error {
	source := S3Connect(LoadTarget(sourceBucketName))
	dest := S3Connect(LoadTarget(destBucketName))

	sourceBucket := source.Connection.Bucket(source.Target.BucketName)
	destBucket := dest.Connection.Bucket(dest.Target.BucketName)

	err := sourceBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	err = destBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	return nil
}

func TestConnection(t *testing.T) {
	conn := S3Connect(LoadTarget(sourceBucketName))

	if conn.Connection == nil {
		t.Error("Could not connect to S3 host.  Check network & credentials")
	}
}

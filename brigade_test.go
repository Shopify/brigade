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

func loadTarget(bucket string) *Target {
	return &Target{os.Getenv("AWS_HOST"), bucket, os.Getenv("ACCESS_KEY"), os.Getenv("SECRET_ACCESS_KEY")}
}

func LoadTestConfig() {
	Config = ConfigType{Source: loadTarget(sourceBucketName), Dest: loadTarget(destBucketName), Workers: 0}
}

type fileFixture struct {
	key  string
	data []byte
	mime string
	perm s3.ACL
}

var sourceFixtures []fileFixture = []fileFixture{
	{"house", []byte("house data"), "text/plain", s3.PublicRead},
	{"house2", []byte("house2 data"), "text/plain", s3.PublicRead},
	{"house3", []byte("house3 data"), "text/xml", s3.PublicRead},
	{"house4", []byte("house4 data"), "text/plain", s3.Private},
	{"house5", []byte("house4 data"), "text/plain", s3.Private}, // only exists in source
	{"animals/cat", []byte("first cat"), "text/plain", s3.PublicRead},
	{"animals/dog", []byte("second cat"), "text/plain", s3.PublicRead}} // only exists in source

var destFixtures []fileFixture = []fileFixture{
	{"house", []byte("house data"), "text/plain", s3.PublicRead},               // identical
	{"house2", []byte("different house2 data"), "text/plain", s3.PublicRead},   // differing data
	{"house3", []byte("house3 data"), "text/xml", s3.PublicRead},               // differing MIME
	{"house4", []byte("house4 data"), "text/plain", s3.PublicRead},             // differing ACL
	{"house6", []byte("house6 data"), "text/plain", s3.Private},                // to be deleted
	{"animals/cat", []byte("first cat"), "text/plain", s3.PublicRead},          // identical
	{"vehicles/truck", []byte("this is a truck"), "text/plain", s3.PublicRead}} // to be deleted

func uploadFixtures(bucket *s3.Bucket, fixtures []fileFixture) error {
	for i := 0; i < len(fixtures); i++ {
		err := bucket.Put(fixtures[i].key, fixtures[i].data, fixtures[i].mime, fixtures[i].perm)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetupBuckets() error {
	source := S3Connect(loadTarget(sourceBucketName))
	dest := S3Connect(loadTarget(destBucketName))

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

	err = uploadFixtures(sourceBucket, sourceFixtures)
	if err != nil {
		return err
	}

	err = uploadFixtures(destBucket, destFixtures)
	if err != nil {
		return err
	}

	return nil
}

func TestConnection(t *testing.T) {
	conn := S3Connect(loadTarget(sourceBucketName))

	if conn == nil {
		t.Error("Could not connect to S3 host.  Check network & credentials")
	}
}

func TestFindKey(t *testing.T) {
	Init()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	sourceList, err := conn.SourceBucket.List("animals/", "/", "", 1000)
	if err != nil {
		t.Error("Failed to list animals dir")
	}

	key, ok := findKey("animals/cat", sourceList)
	if !ok || key.Key != "animals/cat" {
		t.Error("Failed to find animals/cat in source bucket by key")
	}
}

func TestCopyDirectory(t *testing.T) {
	Init()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	conn.CopyDirectory("")

	if ScanDirs.Len() != 1 {
		t.Error("Nothing on ScanDirs queue")
		return
	}
	converted, ok := ScanDirs.Front().Value.(string)
	if !ok || converted != "animals/" {
		t.Error("CopyDirectory failed to push subdirectory onto ScanDirs")
		return
	}

	if DelDirs.Len() != 1 {
		t.Error("Nothing on DelDirs queue")
		return
	}
	converted, ok = DelDirs.Front().Value.(string)
	if !ok || converted != "vehicles/" {
		t.Error("CopyDirectory failed to push subdirectory onto DelDirs")
		return
	}
}

func TestCopyBucket(t *testing.T) {
	Init()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	conn.CopyBucket()

	if ScanDirs.Len() != 0 {
		t.Error("CopyBucket left directories to scan")
		return
	}

	if DelDirs.Len() != 1 {
		t.Error("Nothing on DelDirs queue")
		return
	}

	converted, ok := DelDirs.Front().Value.(string)
	if !ok || converted != "vehicles/" {
		t.Error("CopyBucket failed to push subdirectory onto DelDirs")
		return
	}

	copyExpected := []string{"house2", "house3", "house4", "house5", "animals/dog"}

	if len(CopyFiles) != len(copyExpected) {
		t.Errorf("CopyBucket found %d files to copy but we expected %d", len(CopyFiles), len(copyExpected))
		return
	}

	for i := 0; i < len(copyExpected); i++ {
		file := <-CopyFiles
		if file != copyExpected[i] {
			t.Errorf("CopyBucket file #%d was %s but expected %s", i, file, copyExpected[i])
		}
	}
}

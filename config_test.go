package brigade

import "testing"
import "bytes"

func TestTargetLoadFromJSON(t *testing.T) {
	jsonTarget := bytes.NewBufferString(`
    {
      "Source": {"Server": "s3.com", "BucketName": "source_bucket", "AccessKey": "1234", "SecretAccessKey": "MySecretAccessKey"},
      "Destination": {"Server": "storage.google.com", "BucketName": "google_bucket", "AccessKey": "abcd", "SecretAccessKey": "MyOtherSecretAccessKey"},
      "Workers": 20
    }
  `)

	loadConfig(jsonTarget)

	if env.Source.Server != "s3.com" {
		t.Error("env.Source.Server incorrect")
	}

	if env.Source.BucketName != "source_bucket" {
		t.Error("env.Source.BucketName incorrect")
	}

	if env.Source.AccessKey != "1234" {
		t.Error("env.Source.AccessKey incorrect")
	}

	if env.Source.SecretAccessKey != "MySecretAccessKey" {
		t.Error("env.Source.SecretAccessKey incorrect")
	}

	if env.Destination.Server != "storage.google.com" {
		t.Error("env.Destination.Server incorrect")
	}

	if env.Destination.BucketName != "google_bucket" {
		t.Error("env.Destination.BucketName incorrect")
	}

	if env.Destination.AccessKey != "abcd" {
		t.Error("env.Destination.AccessKey incorrect")
	}

	if env.Destination.SecretAccessKey != "MyOtherSecretAccessKey" {
		t.Error("env.Destination.SecretAccessKey incorrect")
	}

	if env.Workers != 20 {
		t.Error("env.Workers incorrect")
	}
}

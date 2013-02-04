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

	if Config.Source.Server != "s3.com" {
		t.Error("Config.Source.Server incorrect")
	}

	if Config.Source.BucketName != "source_bucket" {
		t.Error("Config.Source.BucketName incorrect")
	}

	if Config.Source.AccessKey != "1234" {
		t.Error("Config.Source.AccessKey incorrect")
	}

	if Config.Source.SecretAccessKey != "MySecretAccessKey" {
		t.Error("Config.Source.SecretAccessKey incorrect")
	}

	if Config.Destination.Server != "storage.google.com" {
		t.Error("Config.Destination.Server incorrect")
	}

	if Config.Destination.BucketName != "google_bucket" {
		t.Error("Config.Destination.BucketName incorrect")
	}

	if Config.Destination.AccessKey != "abcd" {
		t.Error("Config.Destination.AccessKey incorrect")
	}

	if Config.Destination.SecretAccessKey != "MyOtherSecretAccessKey" {
		t.Error("Config.Destination.SecretAccessKey incorrect")
	}

	if Config.Workers != 20 {
		t.Error("Config.Workers incorrect")
	}
}

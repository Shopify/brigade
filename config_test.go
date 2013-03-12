package main

import "testing"
import "bytes"

func TestTargetLoadFromJSON(t *testing.T) {
	jsonTarget := bytes.NewBufferString(`
    {
      "Source": {"Server": "s3.com", "BucketName": "source_bucket", "AccessKey": "1234", "SecretAccessKey": "MySecretAccessKey"},
      "Dest": {"Server": "storage.google.com", "BucketName": "google_bucket", "AccessKey": "abcd", "SecretAccessKey": "MyOtherSecretAccessKey"},
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

	if Config.Dest.Server != "storage.google.com" {
		t.Error("Config.Dest.Server incorrect")
	}

	if Config.Dest.BucketName != "google_bucket" {
		t.Error("Config.Dest.BucketName incorrect")
	}

	if Config.Dest.AccessKey != "abcd" {
		t.Error("Config.Dest.AccessKey incorrect")
	}

	if Config.Dest.SecretAccessKey != "MyOtherSecretAccessKey" {
		t.Error("Config.Dest.SecretAccessKey incorrect")
	}

	if Config.Workers != 20 {
		t.Error("Config.Workers incorrect")
	}
}

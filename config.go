package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aybabtme/goamz/aws"
	"io"
)

// BktConfig is the information needed to create an s3.Bucket object.
type BktConfig struct {
	Region    string `json:"aws_region"`
	AccessKey string `json:"aws_access_key"`
	SecretKey string `json:"aws_secret_key"`
}

func (a *BktConfig) validate() error {

	switch {
	case a.Region == "":
		return errors.New("need an region name")
	case a.AccessKey == "":
		return errors.New("need an access key")
	case a.SecretKey == "":
		return errors.New("need a secret key")
	}

	if _, ok := aws.Regions[a.Region]; !ok {
		var valids []string
		for key := range aws.Regions {
			valids = append(valids, key)
		}
		return fmt.Errorf("not a valid AWS region, valid regions: %v", valids)
	}

	return nil
}

// AWS returns an auth and region object for the bucket.
func (a *BktConfig) AWS() (aws.Auth, aws.Region) {
	return aws.Auth{
		AccessKey: a.AccessKey,
		SecretKey: a.SecretKey,
	}, aws.Regions[a.Region]
}

// Config contains authentication info for the AWS buckets. We use
// a file instead of flags to avoid showing the secrets in the process
// name.
type Config struct {
	Source      BktConfig `json:"source_bucket"`
	Destination BktConfig `json:"destination_bucket"`
	State       BktConfig `json:"state_bucket"`
}

func (c Config) validate() error {
	if err := c.Source.validate(); err != nil {
		return fmt.Errorf("source config is not valid: %v", err)
	}
	if err := c.Destination.validate(); err != nil {
		return fmt.Errorf("destination config is not valid: %v", err)
	}
	if err := c.State.validate(); err != nil {
		return fmt.Errorf("state config is not valid: %v", err)
	}
	return nil
}

// LoadConfig from a reader containing a JSON object, then validate it.
func LoadConfig(r io.Reader) (*Config, error) {
	var c Config
	err := json.NewDecoder(r).Decode(&c)
	if err != nil {
		return nil, fmt.Errorf("decoding json from config: %v", err)
	}
	return &c, c.validate()
}

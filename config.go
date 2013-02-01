package brigade

import "os"
import "io"
import "log"
import "encoding/json"

type Target struct {
	Server          string
	BucketName      string
	AccessKey       string
	SecretAccessKey string
}

type Config struct {
	Source      Target
	Destination Target
	Workers     int
}

var env Config

func readConfig() {
	configFile := os.Getenv("ENV")

	if configFile == "" {
		configFile = "config/default.json"
	}

	log.Printf("Loading environment from %s (override with ENV=xxx)", configFile)

	f, err := os.Open(configFile)

	if err != nil {
		log.Printf("Error opening config file: %s", err)
		return
	}

	loadConfig(f)
}

func loadConfig(source io.Reader) {
	err := json.NewDecoder(source).Decode(&env)

	if err != nil {
		log.Fatalf("Error parsing config file: %s", err)
		return
	}
}

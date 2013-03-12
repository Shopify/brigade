package brigade

import (
  "github.com/tobi/airbrake-go"
  "github.com/shopify/stats/env"
	"errors"
	"fmt"
	"log"
	"os"
)

func main() {
	airbrake.Endpoint = "https://exceptions.shopify.com/notifier_api/v2/notices.xml"
	airbrake.ApiKey = "795dbf40b8743457f64fe9b9abc843fa"

	if len(env.Get("log")) > 0 {
		logFile, err := os.OpenFile(env.Get("log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open log file %s for writing: %s", env.Get("log"), err.Error()))
			airbrake.Error(err, nil)
			log.Fatal(err)
		}
		log.SetOutput(logFile)
		defer logFile.Close()
	}

  log := 300

	if log > 0 {
		go statsWorker(log)
	}

  readConfig()

  Init()
  bucketCopier := S3Init()
  bucketCopier.CopyBucket()
}


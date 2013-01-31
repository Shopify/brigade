package brigade

import "testing"
import "bytes"

func TestTargetLoadFromJSON(t *testing.T) {
  jsonTarget := bytes.NewBufferString(`
    {"Server": "s3.com", "BucketName": "storage_bucket", "AccessKey": "1234", "SecretAccessKey": "MySecretAccessKey"}
  `)

  loadConfig(jsonTarget)

  t.Error("this is an error")
}

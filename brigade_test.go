package brigade

import "os"
import "testing"

func TestSanity(t *testing.T) {
	if os.Getenv("ACCESS_KEY") == "" || os.Getenv("SECRET_ACCESS_KEY") == "" ||
		os.Getenv("BUCKET") == "" || os.Getenv("AWS_HOST") == "" {
		t.Error("Please set ACCESS_KEY, SECRET_ACCESS_KEY, BUCKET, and HOST variables for integration tests")
	}
}

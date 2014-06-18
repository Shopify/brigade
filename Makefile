# SSHKEY=""
# DEPLOY_HOST="deploy@brigade.ec2.shopify.com"
SSHKEY=-i $(HOME)/key/antoine-gonan-dev.pem
DEPLOY_HOST=

BRANCH=`git rev-parse --abbrev-ref HEAD`
COMMIT=`git rev-parse --short HEAD`
GOLDFLAGS="-X main.branch $(BRANCH) -X main.commit $(COMMIT)"

setup:
	@go get -u github.com/tools/godep
	@go get -u github.com/golang/lint/golint
	@go get -u github.com/kisielk/errcheck

# http://cloc.sourceforge.net/
cloc:
	@cloc --sdir='Godeps' --not-match-f='Makefile|_test.go' .

# go get github.com/kisielk/errcheck
errcheck:
	@echo "=== errcheck ==="
	@errcheck ./...

vet:
	@echo "==== go vet ==="
	@go vet ./...

# go get github.com/golang/lint/golint
lint:
	@echo "==== go lint ==="
	@golint ./**/*.go

fmt:
	@echo "=== go fmt ==="
	@go fmt ./...

install: test
	@echo "=== go install ==="
	@godep go install -ldflags=$(GOLDFLAGS)

build:
	@echo "=== go build ==="
	@godep go build -ldflags=$(GOLDFLAGS) -o brigade

test: fmt vet lint errcheck
	@echo "=== go test ==="
	@godep go test ./...

deploy: test
	# Compile
	GOARCH=amd64 GOOS=linux godep go build -ldflags=$(GOLDFLAGS) -o brigade
	#GOARCH=amd64 GOOS=linux godep go build -o brigade-s3-diff ./cmd/brigade-s3-diff/
	#GOARCH=amd64 GOOS=linux godep go build -o brigade-s3-sync ./cmd/brigade-s3-sync/

	# Copy binaries
	@scp $(SSHKEY) brigade-s3-list $(DEPLOY_HOST):~/
	#@scp $(SSHKEY) brigade-s3-diff $(DEPLOY_HOST):~/
	#@scp $(SSHKEY) brigade-s3-sync $(DEPLOY_HOST):~/

	# Cleanup binaries
	@rm brigade-s3-list
	#@rm brigade-s3-diff
	#@rm brigade-s3-sync

	# Copy run scripts
	@scp $(SSHKEY) ./cmd/brigade-s3-list/run_list.sh $(DEPLOY_HOST):~/
	#@scp $(SSHKEY) ./cmd/brigade-s3-diff/run_diff.sh $(DEPLOY_HOST):~/
	#@scp $(SSHKEY) ./cmd/brigade-s3-sync/run_sync.sh $(DEPLOY_HOST):~/


.PHONY: setup cloc errcheck vet lint fmt install build test deploy

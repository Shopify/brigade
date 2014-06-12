# SSHKEY=""
# DEPLOY_HOST="deploy@brigade.ec2.shopify.com"
SSHKEY=-i $(HOME)/key/antoine-gonan-dev.pem
DEPLOY_HOST=ubuntu@ec2-54-227-68-25.compute-1.amazonaws.com

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
	@errcheck cmd/...

vet:
	@echo "==== go vet ==="
	@go vet cmd/...

# go get github.com/golang/lint/golint
lint:
	@echo "==== go lint ==="
	@golint ./cmd/**/*.go

fmt:
	@go fmt ./...


deploy: fmt vet lint errcheck
	# Compile
	GOARCH=amd64 GOOS=linux godep go build -o brigade-s3-list ./cmd/brigade-s3-list/
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


.PHONY: cloc lint fmt test deploy

.PHONY: all dist clean
BINARY_NAME := edge
APP_NAME := streamsets-datacollector-edge
VERSION := 3.0.0.0-SNAPSHOT
DIR=.
BuiltDate := `date +%FT%T%z`
BuiltRepoSha := `git rev-parse HEAD`
GOPATH := $(GOPATH)

# Go setup
GO=go
TEST=go test

DEPENDENCIES := github.com/golang/dep/cmd/dep

EXECUTABLES :=dist/bin/$(BINARY_NAME)$(BINARY_EXTENSION)

# Build Binaries setting BuildInfo vars
LDFLAGS :=-ldflags "-X github.com/streamsets/datacollector-edge/container/common.Version=${VERSION} \
    -X github.com/streamsets/datacollector-edge/container/common.BuiltBy=$$USER \
    -X github.com/streamsets/datacollector-edge/container/common.BuiltDate=${BuiltDate} \
    -X github.com/streamsets/datacollector-edge/container/common.BuiltRepoSha=${BuiltRepoSha}"

# Package target
PACKAGE :=$(DIR)/dist/$(APP_NAME)-$$GOOS-$$GOARCH-$(VERSION).tar.gz

DEPENDENCIES_DIR := $(DEPENDENCIES)

.DEFAULT: dist

all: | $(EXECUTABLES)

$(DEPENDENCIES_DIR):
	@echo Downloading $@
	$(GO) get -u $@

dist/bin/$(BINARY_NAME): main.go $(DEPENDENCIES_DIR)

$(EXECUTABLES):
	@echo Downloading Dependencies...
	@$(GOPATH)/bin/dep ensure
	@rm -dRf dist/bin/
	$(GO) build $(LDFLAGS) -o $@ $<
	@cp -n -R $(DIR)/etc/ dist/etc/ 2>/dev/null || :
	@cp -n -R $(DIR)/data/ dist/data/ 2>/dev/null || :
	@rm -rf dist/data/**/**/*.png
	@rm -rf dist/data/**/**/*.md
	@mkdir -p dist/log

test:
	$(TEST) `go list ./... | grep -v /vendor/` -cover

clean:
	@echo Cleaning Workspace...
	rm -dRf dist

clean-vendor:
	@echo Cleaning Vendor...
	rm -dRf vendor

$(PACKAGE): all
	@echo Packaging Binaries...
	@mkdir -p tmp/$(APP_NAME)/bin
	@cp -R dist/bin/. tmp/$(APP_NAME)/bin
	@cp -R $(DIR)/etc/ tmp/$(APP_NAME)/etc/
	@cp -R $(DIR)/data/ tmp/$(APP_NAME)/data/
	@rm -rf tmp/$(APP_NAME)/data/**/**/*.png
	@rm -rf tmp/$(APP_NAME)/data/**/**/*.md
	@mkdir -p tmp/$(APP_NAME)/log
	tar -czvf $@ -C tmp $(APP_NAME);
	@rm -rf tmp

# for (Mac OS X 10.8 and above and iOS)
dist-darwin-amd64:
	export GOOS="darwin"; \
	export GOARCH="amd64"; \
	$(MAKE) dist-build

# for linux 64 bit i386
dist-linux-amd64:
	export GOOS="linux"; \
	export GOARCH="amd64"; \
	$(MAKE) dist-build

# for Raspberry PI Zero W
dist-linux-arm:
	export GOOS="linux"; \
	export GOARCH="arm"; \
	$(MAKE) dist-build

# for windows 64 bit i386
dist-windows-amd64:
	export GOOS="windows"; \
	export GOARCH="amd64"; \
	$(MAKE) BINARY_EXTENSION=.exe dist-build

dist-build: $(PACKAGE)

dist: dist-darwin-amd64

docker-build:
	docker build -t streamsets/datacollector-edge .

dist-all: dist-linux-amd64 dist-linux-arm dist-windows-amd64 dist-darwin-amd64 docker-build test

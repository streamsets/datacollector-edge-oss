.PHONY: all dist clean
BINARY_NAME := dataextractor
APP_NAME := streamsets-dataextractor
VERSION := 0.0.1
DIR=.
BuiltDate := `date +%FT%T%z`
BuiltRepoSha := `git rev-parse HEAD`

# Go setup
GO=go
TEST=go test

DEPENDENCIES := github.com/hpcloud/tail/... \
    github.com/BurntSushi/toml \
    github.com/satori/go.uuid

# Sources and Targets
EXECUTABLES :=dist/bin/$(BINARY_NAME)
# Build Binaries setting BuildInfo vars
LDFLAGS :=-ldflags "-X github.com/streamsets/dataextractor/lib/common.Version=${VERSION} \
    -X github.com/streamsets/dataextractor/lib/common.BuiltBy=$$USER \
    -X github.com/streamsets/dataextractor/lib/common.BuiltDate=${BuiltDate} \
    -X github.com/streamsets/dataextractor/lib/common.BuiltRepoSha=${BuiltRepoSha}"

# Package target
PACKAGE :=$(DIR)/dist/$(APP_NAME)-$(VERSION).tar.gz

DEPENDENCIES_DIR := $(DEPENDENCIES)

.DEFAULT: dist

all: | $(EXECUTABLES)

$(DEPENDENCIES_DIR):
	@echo Downloading $@
	$(GO) get $@

dist/bin/$(BINARY_NAME): main.go $(DEPENDENCIES_DIR)

$(EXECUTABLES):
	$(GO) build $(LDFLAGS) -o $@ $<
	@cp -n -R $(DIR)/etc/ dist/etc 2>/dev/null || :
	@mkdir -p dist/logs
	@mkdir -p dist/data

test:
	$(TEST) -r -cover

clean:
	@echo Cleaning Workspace...
	rm -dRf dist

$(PACKAGE): all
	@echo Packaging Binaries...
	@mkdir -p tmp/$(APP_NAME)/bin
	@cp -R dist/bin/ tmp/$(APP_NAME)/bin
	@cp -R $(DIR)/etc/ tmp/$(APP_NAME)/etc
	@mkdir -p tmp/$(APP_NAME)/logs
	@mkdir -p tmp/$(APP_NAME)/data
	tar -cf $@ -C tmp $(APP_NAME);
	@rm -rf tmp

dist: $(PACKAGE)

# These variables get inserted into ./build/commit.go
BUILD_TIME=$(shell date)
GIT_REVISION=$(shell git rev-parse --short HEAD)
GIT_DIRTY=$(shell git diff-index --quiet HEAD -- || echo "✗-")

ldflags= -X github.com/uplo-tech/writeaheadlog/build.GitRevision=${GIT_DIRTY}${GIT_REVISION} \
-X "github.com/uplo-tech/writeaheadlog/build.BuildTime=${BUILD_TIME}"

# all will build and install release binaries
all: release

# count says how many times to run the tests.
count = 1

# cpkg determines which package is the target when running 'make fullcover'.
# 'make fullcover' can only provide full coverage statistics on a single package
# at a time, unfortunately.
cpkg = ./

# pkgs changes which packages the makefile calls operate on. run changes which
# tests are run during testing.
pkgs = ./

# run determines which tests run when running any variation of 'make test'.
run = .

# dependencies list all packages needed to run make commands used to build, test
# and lint uploc/uplod locally and in CI systems.
dependencies:
	./install-dependencies.sh
	go get -d ./...

# fmt calls go fmt on all packages.
fmt:
	gofmt -s -l -w $(pkgs)

# vet calls go vet on all packages.
# NOTE: go vet requires packages to be built in order to obtain type info.
vet:
	go vet $(pkgs)

# markdown-spellcheck runs codespell on all markdown files that are not
# vendored.
markdown-spellcheck:
	git ls-files "*.md" :\!:"vendor/**" | xargs codespell --check-filenames

# lint runs golangci-lint (which includes golint, a spellcheck of the codebase,
# and other linters), the custom analyzers, and also a markdown spellchecker.
lint: markdown-spellcheck
	golangci-lint run -c .golangci.yml

# lint-ci runs golint.
lint-ci:
# golint is skipped on Windows.
ifneq ("$(OS)","Windows_NT")
# Linux
	go get golang.org/x/lint/golint
	golint -min_confidence=1.0 -set_exit_status $(pkgs)
endif

# spellcheck checks for misspelled words in comments or strings.
spellcheck: markdown-spellcheck
	go get -d ./...
	golangci-lint run -c .golangci.yml -E misspell

# staticcheck runs the staticcheck tool
# NOTE: this is not yet enabled in the CI system.
staticcheck:
	staticcheck $(pkgs)

# debug builds and installs debug binaries. This will also install the utils.
debug:
	go install -tags='debug profile netgo' -ldflags='$(ldflags)' $(pkgs)
debug-race:
	go install -race -tags='debug profile netgo' -ldflags='$(ldflags)' $(pkgs)

# dev builds and installs developer binaries. This will also install the utils.
dev:
	go install -tags='dev debug profile netgo' -ldflags='$(ldflags)' $(pkgs)
dev-race:
	go install -race -tags='dev debug profile netgo' -ldflags='$(ldflags)' $(pkgs)

# release builds and installs release binaries.
release:
	go get -d ./...
	go install -tags='netgo' -ldflags='-s -w $(ldflags)' $(release-pkgs)
release-race:
	go get -d ./...
	go install -race -tags='netgo' -ldflags='-s -w $(ldflags)' $(release-pkgs)

# clean removes all directories that get automatically created during
# development.
clean:
	rm -rf cover fullcover release

test:
	go test -short -tags='debug testing netgo' -timeout=60s $(pkgs) -run=$(run) -count=$(count)
test-v:
	go test -race -v -short -tags='debug testing netgo' -timeout=75s $(pkgs) -run=$(run) -count=$(count)
test-long: clean fmt vet lint-ci
	@mkdir -p cover
	go test --coverprofile='./cover/cover.out' -v -race -failfast -tags='testing debug netgo' -timeout=3600s $(pkgs) -run=$(run) -count=$(count)
test-vlong: clean fmt vet lint-ci
	@mkdir -p cover
	go test --coverprofile='./cover/cover.out' -v -race -tags='testing debug vlong netgo' -timeout=20000s $(pkgs) -run=$(run) -count=$(count)
test-cpu:
	go test -v -tags='testing debug netgo' -timeout=500s -cpuprofile cpu.prof $(pkgs) -run=$(run) -count=$(count)
test-mem:
	go test -v -tags='testing debug netgo' -timeout=500s -memprofile mem.prof $(pkgs) -run=$(run) -count=$(count)
bench: clean fmt
	go test -tags='debug testing netgo' -timeout=500s -run=XXX -bench=$(run) $(pkgs) -count=$(count)
cover: clean
	@mkdir -p cover
	@for package in $(pkgs); do                                                                                                                                 \
		mkdir -p `dirname cover/$$package`                                                                                                                      \
		&& go test -tags='testing debug netgo' -timeout=500s -covermode=atomic -coverprofile=cover/$$package.out ./$$package -run=$(run) || true \
		&& go tool cover -html=cover/$$package.out -o=cover/$$package.html ;                                                                                    \
	done

# fullcover is a command that will give the full coverage statistics for a
# package. Unlike the 'cover' command, full cover will include the testing
# coverage that is provided by all tests in all packages on the target package.
# Only one package can be targeted at a time. Use 'cpkg' as the variable for the
# target package, 'pkgs' as the variable for the packages running the tests.
#
# NOTE: this command has to run the full test suite to get output for a single
# package. Ideally we could get the output for all packages when running the
# full test suite.
#
# NOTE: This command will not skip testing packages that do not run code in the
# target package at all. For example, none of the tests in the 'sync' package
# will provide any coverage to the renter package. The command will not detect
# this and will run all of the sync package tests anyway.
fullcover: clean
	@mkdir -p fullcover
	@mkdir -p fullcover/tests
	@echo "mode: atomic" >> fullcover/fullcover.out
	@for package in $(pkgs); do                                                                                                                                                             \
		mkdir -p `dirname fullcover/tests/$$package`                                                                                                                                        \
		&& go test -tags='testing debug netgo' -timeout=500s -covermode=atomic -coverprofile=fullcover/tests/$$package.out -coverpkg $(cpkg) ./$$package -run=$(run) || true \
		&& go tool cover -html=fullcover/tests/$$package.out -o=fullcover/tests/$$package.html                                                                                              \
		&& tail -n +2 fullcover/tests/$$package.out >> fullcover/fullcover.out ;                                                                                                            \
	done
	@go tool cover -html=fullcover/fullcover.out -o fullcover/fullcover.html
	@printf 'Full coverage on $(cpkg):'
	@go tool cover -func fullcover/fullcover.out | tail -n -1 | awk '{$$1=""; $$2=""; sub(" ", " "); print}'

.PHONY: all fmt install release clean test test-v test-long cover whitepaper

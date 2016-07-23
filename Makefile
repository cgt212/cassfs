MAIN_PACKAGE := cassfs
GO_LINUX := GOOS=linux GOARCH=amd64
GO_OSX := GOOS=darwin GOARCH=amd64
BUILT_ON := $(shell date)
COMMIT_HASH := $(shell git log -n 1 --pretty=format:"%H")
LDFLAGS := '-X "main.BuiltOn=$(BUILT_ON)" -X "main.CommitHash=$(COMMIT_HASH)" -s -w'


export GO15VENDOREXPERIMENT=1

test:
	go test -p=1 -cover `go list ./... | sed -n '1!p' | grep -v /vendor/` -v

osx:
	go generate ./...
	$(GO_OSX) go build -o cassfs -ldflags $(LDFLAGS) .

linux:
	go generate ./...
	$(GO_LINUX) go build -o cassfs -ldflags $(LDFLAGS) .

clean:
	find . -name *_gen.go -type f -exec rm {} \;
	rm -f ./cassfs

run:
	go generate ./...
	go run -ldflags $(LDFLAGS) main.go

fmt:
	go fmt ./...

MAIN_PACKAGE := cassfs
GO_LINUX := GOOS=linux GOARCH=amd64
GO_OSX := GOOS=darwin GOARCH=amd64

export GO15VENDOREXPERIMENT=1

test:
	go test -p=1 -cover `go list ./... | sed -n '1!p' | grep -v /vendor/` -v

osx:
	go generate ./...
	$(GO_OSX) go build -o cassfs

linux:
	go generate ./...
	$(GO_LINUX) go build -o cassfs

clean:
	find . -name *_gen.go -type f -exec rm {} \;
	rm -f ./cassfs

run:
	go generate ./...
	go run main.go

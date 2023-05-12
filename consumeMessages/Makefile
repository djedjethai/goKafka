
.PHONY: build
build:
	# CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o main .
	go build -o kfk .
	# CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -ldflags="-extld=aarch64-linux-gnu-gcc"

.PHONY: compile
compile:
	protoc ./api/v1/proto/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.



.PHONY: build
build:
	go build -o kfk .

.PHONY: compile
compile:
	protoc ./*.proto \
		--go_out=./plugin \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.



.PHONY: build
build:
	cd ./app/bin && go build -o kfk ..

.PHONY: compile
compile:
	protoc ./app/api/v1/proto/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.


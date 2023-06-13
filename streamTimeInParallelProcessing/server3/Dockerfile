#####################################
#   STEP 1 build executable binary  #
#####################################
FROM golang:1.20.4-alpine3.17 AS builder

# Install git.
# Git is required for fetching the dependencies.
RUN apk update && \
	apk add --no-cache git gcc  && \
	apk add --update gcc musl-dev

WORKDIR /app

COPY go.mod .
COPY go.sum .

# RUN go get github.com/riferrei/srclient@v0.5.4
RUN go mod download

COPY . .

# Build the binary.
# enabled CGO_ENABLED=1 for the C layer of kafka to be compile as well
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -ldflags '-extldflags "-static"' -o main
# RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -ldflags '-extldflags "-static"' -o main

#####################################
#   STEP 2 build a small image      #
#####################################
FROM golang:alpine

# Copy our static executable.
COPY --from=builder /app/main /app/main

# Run the hello binary.
ENTRYPOINT ["/app/main"]



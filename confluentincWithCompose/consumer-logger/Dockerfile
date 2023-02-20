#####################################
#   STEP 1 build executable binary  #
#####################################
FROM golang:alpine AS builder

# Install git.
# Git is required for fetching the dependencies.
RUN apk update && \
	apk add --no-cache git && \
	apk add --update gcc musl-dev 

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go mod tidy

# Build the binary.
RUN CGO_ENABLED=1 GOOS=linux go build -o main -tags musl

#####################################
#   STEP 2 build a small image      #
#####################################
FROM golang:alpine

# Copy our static executable.
COPY --from=builder /app/main /app/main

# Run the hello binary.
ENTRYPOINT ["/app/main"]


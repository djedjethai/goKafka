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

# Build the binary.
# enabled CGO_ENABLED=1 for the C layer of kafka to be compile as well
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o main -tags musl

#####################################
#   STEP 2 build a small image      #
#####################################
FROM golang:alpine

# Copy our static executable.
COPY --from=builder /app/main /app/main

# Run the hello binary.
ENTRYPOINT ["/app/main"]













# see if userAccount and certif, interesting...
# FROM golang:1.16.4-alpine AS builder
# 
# RUN apk update \
#     && apk add --no-cache git \
#     && apk add --no-cache ca-certificates \
#     && apk add --update gcc musl-dev \
#     && update-ca-certificates
# 
# # Create a user so that the image doens't run as root
# RUN adduser \
#     --disabled-password \
#     --gecos "" \
#     --home "/nonexistent" \
#     --shell "/sbin/nologin" \
#     --no-create-home \
#     --uid "100001" \
#     "appuser"
# 
# # Set the working directory inside the container.
# WORKDIR $GOPATH/src/app
# 
# # Copy all files from the current directory to the working directory
# COPY . .
# 
# # Fetch dependencies.
# RUN go get -u -d -v
# 
# # Go build the binary, specifying the final OS and architecture we're looking for
# RUN GOOS=linux CGO_ENABLED=1 GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/app -tags timetzdata
# 
# FROM scratch
# 
# # Import the user and group files from the builder.
# COPY --from=builder /etc/passwd /etc/passwd
# COPY --from=builder /etc/group /etc/group
# COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# 
# # Copy our static executable.
# COPY --from=builder /go/bin/app/go/bin/app
# 
# # Use the user that we've just created, one that isn't root
# USER appuser:appuser
# 
# ENTRYPOINT ["/go/bin/app"]

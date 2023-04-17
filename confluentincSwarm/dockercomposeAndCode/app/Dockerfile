#####################################
#   STEP 1 build executable binary  #
#####################################
FROM golang:1.20-alpine AS builder

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


# from goMicro
# FROM golang:alpine
# 
# # Copy our static executable.
# # COPY --from=builder /app/main /app/main
# RUN mkdir /app
# 
# # COPY --from=builder /app/brokerApp /app
# COPY ./bin/kfk /app
# RUN ls /
# RUN ls /app
# 
# # Run the hello binary.
# ENTRYPOINT ["/app/kfk"]


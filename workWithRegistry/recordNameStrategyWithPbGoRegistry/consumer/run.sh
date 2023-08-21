#!/bin/bash

cd ./loadPbFiles
go build -o loader .
./loader

cd ../consumerRun
go build -o kfk .
./kfk


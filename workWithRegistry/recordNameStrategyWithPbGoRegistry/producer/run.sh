#!/bin/bash

cd ./loadPbFiles
go build -o loader .
./loader

cd ../producerRun
go build -o kfk .
./kfk

#!/bin/bash

cd ./loadPbFiles
go build -o pbfiles .
./pbfiles

cd ../producerRun
go build -o kfk .
./kfk

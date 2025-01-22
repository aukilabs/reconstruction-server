#!/bin/bash

cd server && go build -buildvcs=false

cp reconstruction ../
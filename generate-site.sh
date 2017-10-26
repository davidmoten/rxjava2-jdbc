#!/bin/bash
set -e
NAME=${PWD##*/}
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p $NAME
cp -r ../$NAME/target/site/* $NAME/
git add .
git commit -am "update site reports"
git push
cd ../$NAME

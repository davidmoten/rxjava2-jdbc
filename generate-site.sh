#!/bin/bash
set -e
cd rxjava2-jdbc
NAME=${PWD##*/}
mvn site
cd ../../davidmoten.github.io
git pull
mkdir -p $NAME
cp -r ../$NAME/rxjava2-jdbc/target/site/* $NAME/
git add .
git commit -am "update site reports"
git push
cd ../$NAME

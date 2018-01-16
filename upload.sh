#!/usr/bin/env bash

cd `dirname $0`
cd pard
mvn clean
mvn package
cd ..
scp pard/pard-main/target/pard-server.jar pard@10.77.40.41:/home/pard/pard-src/pard-server.jar
scp pard/pard-client/target/pard-client.jar pard@10.77.40.41:/home/pard/pard-src/pard-client.jar
ssh pard@10.77.40.41 /home/pard/pard-src/deploy_2.sh

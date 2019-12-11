#!/bin/sh
NAME="club-1"
VOLUMES=" -v /${HOME}/.ssh:/root/.ssh -v /dev/urandom:/dev/random "
PORTMAPPINGS="-p 8080:8181 -p 8101:8101 -p 5701:5701 "
CONTAINER="dexels/navajo:3.7.2"
ENV='-e TZ=Europe/Amsterdam -e CONTAINERNAME=club-1 -e INSTANCENAME=club-1 -e CONTAINER=dexels/navajo:3.6.22 -e GIT_REPOSITORY_TYPE=tipi -e GIT_REPOSITORY_FILEINSTALL=etc,etc-club -e GIT_REPOSITORY_DEPLOYMENT=any -e GIT_REPOSITORY_BRANCH=cachetest -e TIPI_STORE_CODEBASE=http://docker.local:8080 -e TIPI_STORE_MANIFESTCODEBASE=docker.local -e JAVA_MAX_MEM=130M -e CLUSTER=club -e GIT_REPOSITORY_SLEEPTIME=10000 -e TIPI_STORE_AUTHORIZE=false -e GIT_REPOSITORY_URL=git@github.com:Dexels/club-deploy -e TIPI_STORE_ORGANIZATION=Dexels '

echo -n "Starting $CONTAINER as $NAME: "
docker rm $NAME 2>/dev/null
docker run --name $NAME ${LINK} ${VOLUMES} ${PORTMAPPINGS} ${ENV} ${SETTINGS} -i -t ${CONTAINER} ${PARAMETERS}

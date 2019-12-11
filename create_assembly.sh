#!/bin/sh
mkdir -p assembly
rm -rf assembly/*
find . -name *.jar | grep generated | xargs -I {} cp {} assembly
cd assembly
mkdir -p core
mkdir -p oauth
mkdir -p pubsub
mkdir -p appstore
mkdir -p rackermon
mkdir -p experimental
mkdir -p streams

ls *.jar | grep immutable | xargs -I {} mv {} core
ls *.jar | grep repository | xargs -I {} mv {} core
ls *.jar | grep replication | xargs -I {} mv {} core
ls *.jar | grep hazelcast | xargs -I {} mv {} core
ls *.jar | grep mgmt | xargs -I {} mv {} core
ls *.jar | grep resourcebundle | xargs -I {} mv {} core
ls *.jar | grep bundlesync | xargs -I {} mv {} core
ls *.jar | grep logback | xargs -I {} mv {} core
ls *.jar | grep appstore | xargs -I {} mv {} appstore
ls *.jar | grep keystoreprovider | xargs -I {} mv {} appstore
ls *.jar | grep log4j | xargs -I {} mv {} appstore
ls *.jar | grep oauth | xargs -I {} mv {} oauth
ls *.jar | grep rackermon | xargs -I {} mv {} rackermon
ls *.jar | grep slack | xargs -I {} mv {} rackermon
ls *.jar | grep packagestream | xargs -I {} mv {} experimental
ls *.jar | grep streams | xargs -I {} mv {} streams
ls *.jar | grep kafka | xargs -I {} mv {} pubsub
ls *.jar | grep pubsub | xargs -I {} mv {} pubsub
ls *.jar | grep mqtt | xargs -I {} mv {} pubsub
mv *.jar experimental

tar cvfz appstore.tgz -C appstore .
tar cvfz core.tgz -C core .
tar cvfz oauth.tgz -C oauth .
tar cvfz pubsub.tgz -C pubsub .
tar cvfz rackermon.tgz -C rackermon .
tar cvfz experimental.tgz -C experimental .
tar cvfz streams.tgz -C streams .
rm -rf core oauth pubsub appstore rackermon experimental streams

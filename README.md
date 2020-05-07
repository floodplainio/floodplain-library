# Welcome to Floodplain!

Floodplain is a Kotlin based stream processing framework, check [https://www.floodplain.io/](https://www.floodplain.io/) for in-depth documentation.

Circle CI Build Status:

[![CircleCI](https://circleci.com/gh/floodplainio/floodplain-library.svg?style=svg)](https://circleci.com/gh/floodplainio/floodplain-library)

Available on Maven Central:
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/cz.jirutka.rsql/rsql-parser/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.floodplain/floodplain-dsl)

## Building

Floodplain requires Gradle and Java 11+

```bash
gradle build
```

... should do the trick

## Running Examples

Make sure the demo setup is running:

https://github.com/floodplainio/floodplain-demo-setup

Follow the README, it should be easy to start.
After running an example, stop, delete and restart the demo with

```bash
CTRL+C
docker-compose rm
```

Import this project into IntelliJ, other IDE's should be easy to import (assuming there is some kind of gradle integration).

Find, for example, the FloodplainAddresses.kt example file.
Right-click and run, this should create some log messages and then keep running.

Open a MongoDB client, point it to localhost:27017, and it will have created a new database. When you keep the FloodplainAddresses running, you can make changes to Postgres, and see the changes reflected in MongoDB.

- TODO Separate examples into another project

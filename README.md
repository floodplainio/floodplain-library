# Welcome to Floodplain!

Floodplain 1.3.1 is out. Notable features:

- More consistent handling of date types. Floodplain does no longer uses the old java.util.Date class. It now uses: DATE / TIMESTAMP / CLOCKTIME types that map to LocalDate, LocalDateTime and LocalTime.
- Support for FHIR based sources (i.e. Kafka Topics in FHIR / JSON format)

Floodplain is a Kotlin based stream processing framework, check [https://www.floodplain.io/](https://www.floodplain.io/) for in-depth documentation.

Circle CI Build Status:

[![CircleCI](https://circleci.com/gh/floodplainio/floodplain-library.svg?style=svg)](https://circleci.com/gh/floodplainio/floodplain-library)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Ffloodplainio%2Ffloodplain-library.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Ffloodplainio%2Ffloodplain-library?ref=badge_shield)

Available on Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.floodplain/floodplain-dsl/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.floodplain/floodplain-dsl)

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
After running an example, stop, delete and restart the demo with:

```bash
CTRL+C
docker-compose rm
```

Import this project into IntelliJ. Other IDE's should be easy to import (assuming there is some kind of gradle integration).

Find, for example, the FloodplainAddresses.kt example file.
Right-click and run, this should create some log messages and then keep running.

Open a MongoDB client, point it to localhost:27017, and it will have created a new database. When you keep the FloodplainAddresses running, you can make changes to Postgres, and see the changes reflected in MongoDB.

- TODO Separate examples into another project


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Ffloodplainio%2Ffloodplain-library.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Ffloodplainio%2Ffloodplain-library?ref=badge_large)
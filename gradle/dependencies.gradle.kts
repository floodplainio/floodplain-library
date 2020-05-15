ext {
    floodplain_version = "0.8.30"
    jackson_version = "2.10.3"
    kafka_version = "2.5.0"
    mongodb_version = "3.12.2"
    slf4j_version = "1.7.30"
    mysql_version = "8.0.18"
    debezium_version = "1.0.0.Final"
    commonDependencies = [
            cdiApi            : "jakarta.enterprise:jakarta.enterprise.cdi-api:2.0.2",
            microProfileConfig: "org.eclipse.microprofile.config:microprofile-config-api:1.3",
            jUnit             : "junit:junit:4.12",
            slf4j             : "org.slf4j:slf4j-api:$slf4j_version",
            slf4jSimple       : "org.slf4j:slf4j-simple:$slf4j_version",
            slf4jLog4j        : "org.slf4j:slf4j-log4j12:$slf4j_version",
            servletApi        : "javax.servlet:javax.servlet-api:3.1.0",
            protobuf          : "com.google.protobuf:protobuf-java:3.11.4",
            kotlinLogging     : 'io.github.microutils:kotlin-logging:1.7.9',
            jacksonCore       : "com.fasterxml.jackson.core:jackson-core:$jackson_version",
            jacksonDatabind   : "com.fasterxml.jackson.core:jackson-databind:$jackson_version",
            kafkaClient       : "org.apache.kafka:kafka-clients:$kafka_version",
            kafkaConnectApi   : "org.apache.kafka:connect-api:$kafka_version",
            kafkaConnectRuntime : "org.apache.kafka:connect-runtime:$kafka_version",
            kafkaStreams      : "org.apache.kafka:kafka-streams:$kafka_version",
            kafkaStreamsTestUtils : "org.apache.kafka:kafka-streams-test-utils:$kafka_version"
    ]

}
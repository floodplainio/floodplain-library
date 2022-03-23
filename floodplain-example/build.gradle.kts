import io.floodplain.build.FloodplainDeps

plugins {
    // Apply the java-library plugin to add support for Java Library
    id("java")
    id("application")
}

dependencies {
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-googlesheets"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-jdbc"))

    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":replication-api"))
    implementation(project(":replication-json"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.commonsCompress) // update for vulnerability
    testImplementation(FloodplainDeps.log4j2)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}

import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation(FloodplainDeps.jacksonCore)
    implementation(FloodplainDeps.jacksonDatabind)
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation("ca.uhn.hapi.fhir:hapi-fhir-base:${FloodplainDeps.hapiFhir}")
    implementation("ca.uhn.hapi.fhir:hapi-fhir-structures-r4:${FloodplainDeps.hapiFhir}")
    implementation("ca.uhn.hapi.fhir:org.hl7.fhir.r4:${FloodplainDeps.hapiFhir}")
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")  // "org.junit.jupiter:junit-jupiter:5.5.2"
}

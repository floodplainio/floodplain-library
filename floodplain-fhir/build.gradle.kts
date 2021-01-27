import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    testCompile(FloodplainDeps.jUnit)
    implementation(FloodplainDeps.jacksonCore)
    implementation(FloodplainDeps.jacksonDatabind)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation("ca.uhn.hapi.fhir:hapi-fhir-base:5.2.0")
    implementation("ca.uhn.hapi.fhir:hapi-fhir-structures-r4:5.2.0")
    implementation("ca.uhn.hapi.fhir:org.hl7.fhir.r4:5.2.0")

    implementation(FloodplainDeps.kotlinCoroutines)
}


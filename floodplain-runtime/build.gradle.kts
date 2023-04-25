import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":floodplain-dsl"))
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}

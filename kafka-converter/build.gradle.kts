import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import io.floodplain.build.FloodplainDeps

plugins {
    id("com.github.johnrengelman.shadow")
}

tasks {
    "shadowJar"(ShadowJar::class) {
//        baseName = project.name
//        classifier = null
//        version = null
        mergeServiceFiles {
            exclude("META-INF/services/javax.xml.stream.*")
        }
    }
}

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    testCompile(FloodplainDeps.jUnit)
    compile(FloodplainDeps.kafkaStreams)
    compile(FloodplainDeps.kafkaConnectApi)
}

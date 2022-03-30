import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.proto
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import io.floodplain.build.FloodplainDeps

val protobufVersion = "3.19.2"

dependencies {
    implementation(FloodplainDeps.jacksonCore)
    implementation(FloodplainDeps.jacksonDatabind)
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.cdiApi)
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":streams-api"))
    implementation("com.github.spotbugs:spotbugs-annotations:4.0.1")
    implementation(FloodplainDeps.protobuf)
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        // exclude(group = "log4j", module = "log4j" )
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender" )
    }
    implementation(FloodplainDeps.kafkaTools) {
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender" )
    }
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
plugins {
    id("com.google.protobuf")
}

//
sourceSets {
    create("proto") {
        proto {
            srcDir("src/main/proto")
        }
    }
}
//
// tasks.withType<com.google.protobuf.gradle.GenerateProtoTask>().configureEach {
//
// }
//

// tasks.compileJava {
//     dependsOn(tasks.withType<com.google.protobuf.gradle.ProtobufConfigurator.GenerateProtoTaskCollection>())
// }

tasks.check { dependsOn("generateProto") }

tasks.withType<JavaCompile>() {
    if(name.contains("compileProtoJava")) {
        enabled = false
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    generateProtoTasks {
        // val a: JavaCompile
        // all().withType<JavaCompile>() { r ->
        //     // if(task.name.contains("compileProtoJava")) {
        //     //     task.enabled = false
        //     // }
        // }
        // all().each { task ->
        //     task.builtins {
        //         java { option 'lite' }
        //     }
        //     task.plugins {
        //         grpc { option 'lite' }
        //     }
        // }
        // this.
        // ofSourceSet("main").plugins {
        //     // Apply the "grpc" plugin whose spec is defined above, without
        //     // options.  Note the braces cannot be omitted, otherwise the
        //     // plugin will not be added. This is because of the implicit way
        //     // NamedDomainObjectContainer binds the methods.
        // }
    }
}
tasks.jacocoTestReport {
    dependsOn(tasks.test)
    classDirectories.setFrom(
        files(classDirectories.files.map {
            fileTree(it) {
                exclude("**/generated/**")
            }
        })
    )
}
import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project("::streams-api"))
    compile(FloodplainDeps.protobuf)
    testCompile(FloodplainDeps.jUnit)
}
//protobuf {
//    protoc {
//        artifact = "com.google.protobuf:protoc:3.11.4"
//    }
//    generateProtoTasks {
//        ofSourceSet("main")*.plugins {
//            // Apply the "grpc" plugin whose spec is defined above, without
//            // options.  Note the braces cannot be omitted, otherwise the
//            // plugin will not be added. This is because of the implicit way
//            // NamedDomainObjectContainer binds the methods.
//        }
//    }
//}

plugins {
    // Apply the java-library plugin to add support for Java Library
    id ("java")
}
apply(from="${rootProject.projectDir}/gradle/shared.gradle.kts")
val globalConf = rootProject.ext

dependencies {
    val dependencies: Map<String, String> = globalConf["commonDependencies"] as Map<String, String>
    compile(dependencies["commonDependencies"] as String)
}

apply(plugin="com.github.hierynomus.license-base")
// license {
//     header(rootProject.file("gradle/LICENSE_HEADER.txt"))
//     mapping("java", "SLASHSTAR_STYLE")
//     mapping("kt", "SLASHSTAR_STYLE")
//     ext.year = "2020"
//     ext.name = "Frank Lyaruu"
// }

configure<SourceSetContainer> {
    val main by getting
    named("main") {
        compileClasspath += main.output
        runtimeClasspath += main.output
    }
}

//
// task publishAll {
//     dependsOn "build"
//     dependsOn "javadocJar"
//     dependsOn "sourceJar"
//
//     doLast {
//         println "We release now"
//     }
// }

import org.gradle.kotlin.dsl.*

plugins {
    java
    kotlin("jvm")
}

dependencies {
    annotationProcessor(Libs.lombok)

    compile(Libs.jfix_stdlib_socket)
    compile(Libs.jackson_annotations)
    compile(Libs.jackson_datatype_jsr310)
    compile(Libs.jackson_kotlin_module)
    compile(Libs.apache_curator)
    compile(Libs.apache_curator_test)

    compileOnly(Libs.lombok)

    testImplementation(Libs.logback)
    testImplementation(Libs.commons_io)
    testImplementation(Libs.junit4)
}

tasks.test {
    useJUnit()
}

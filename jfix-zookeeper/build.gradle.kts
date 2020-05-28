import org.gradle.kotlin.dsl.*

plugins {
    java
    kotlin("jvm")
}

dependencies {
    annotationProcessor(Libs.lombok)

    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.jackson_annotations)
    implementation(Libs.jackson_datatype_jsr310)
    implementation(Libs.jackson_kotlin_module)
    implementation(Libs.apache_curator)
    implementation(Libs.apache_curator_test)

    compileOnly(Libs.lombok)

    testImplementation(Libs.logback)
    testImplementation(Libs.commons_io)
    testImplementation(Libs.junit_jupiter_engine)
    testImplementation(Libs.junit_jupiter_api)
}

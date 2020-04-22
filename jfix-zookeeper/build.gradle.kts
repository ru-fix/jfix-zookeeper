import org.gradle.kotlin.dsl.*

plugins {
    java
    kotlin("jvm")
}

dependencies {
    annotationProcessor(Libs.lombok)

    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.aggregating_profiler)
    implementation(Libs.jfix_concurrency)
    implementation(Libs.jfix_dynamic_property_api)

    implementation(Libs.jackson_annotations)
    implementation(Libs.jackson_datatype_jsr310)
    implementation(Libs.jackson_kotlin_module)
    implementation(Libs.apache_curator)
    implementation("org.apache.curator:curator-x-discovery:4.3.0")
    implementation(Libs.apache_curator_test)
    implementation(Libs.awaitility)

    compileOnly(Libs.lombok)

    testImplementation(Libs.logback)
    testImplementation(Libs.commons_io)
    testImplementation(Libs.junit_jupiter_engine)
    testImplementation(Libs.junit_jupiter_api)
}

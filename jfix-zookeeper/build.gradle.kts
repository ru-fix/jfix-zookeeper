plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.kotlinx_coroutines_core)

    annotationProcessor(Libs.lombok)

    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.aggregating_profiler)
    implementation(Libs.jfix_concurrency)
    implementation(Libs.jfix_dynamic_property_api)

    implementation(Libs.log4j_kotlin)
    implementation(Libs.log4j_core)
    implementation(Libs.slf4j_over_log4j)

    implementation(Libs.jackson_annotations)
    implementation(Libs.jackson_datatype_jsr310)
    implementation(Libs.jackson_kotlin_module)
    implementation(Libs.apache_curator)
    implementation(Libs.apache_curator_discovery)
    implementation(Libs.apache_curator_test)

    compileOnly(Libs.lombok)

    testImplementation(Libs.commons_io)
    testImplementation(Libs.junit_jupiter_engine)
    testImplementation(Libs.junit_jupiter_api)
}

plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.slf4j_api)
    implementation(Libs.jackson_annotations)
    implementation(Libs.jackson_datatype_jsr310)
    implementation(Libs.jackson_kotlin_module)
    implementation(Libs.apache_curator_recipes)

    testImplementation(Libs.log4j_slf4j_impl)
    testImplementation(Libs.kotlinx_coroutines_core)
    testImplementation(Libs.junit_jupiter_engine)
    testImplementation(Libs.junit_jupiter_api)
    testImplementation(Libs.junit_jupiter_params)
    testImplementation(project(":jfix-zookeeper-test"))
}

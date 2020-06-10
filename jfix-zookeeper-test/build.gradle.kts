plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.kotlin_jdk8)
    implementation(Libs.kotlin_stdlib)
    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.slf4j_api)
    implementation(Libs.apache_curator_recipes)
    implementation(Libs.apache_curator_test)
    implementation(Libs.netcrusher)
}

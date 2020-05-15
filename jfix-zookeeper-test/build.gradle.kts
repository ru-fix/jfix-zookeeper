plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.slf4j_api)
    implementation(Libs.apache_curator_recipes)
    implementation(Libs.apache_curator_test)
}

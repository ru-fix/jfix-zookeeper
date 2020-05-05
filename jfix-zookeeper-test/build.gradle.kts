plugins {
    java
    kotlin("jvm")
}

dependencies {
    implementation(Libs.jfix_stdlib_socket)
    implementation(Libs.slf4j)
    implementation(Libs.apache_curator)
    implementation(Libs.apache_curator_test)
}

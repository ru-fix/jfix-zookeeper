
object Vers {
    val kotlin = "1.3.41"
    val sl4j = "1.7.25"
    val dokka = "0.9.18"
    val gradle_release_plugin = "1.3.8"
    val junit = "5.2.0"
    val hamkrest = "1.4.2.2"
}

object Libs {
    val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokka}"

    val slf4j_api = "org.slf4j:slf4j-api:${Vers.sl4j}"
    val slf4j_simple = "org.slf4j:slf4j-simple:${Vers.sl4j}"

    val mockito = "org.mockito:mockito-all:1.10.19"
    val mockito_kotiln = "com.nhaarman:mockito-kotlin-kt1.1:1.5.0"
    val kotlin_logging = "io.github.microutils:kotlin-logging:1.4.9"

    val junit_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    val junit_parametri = "org.junit.jupiter:junit-jupiter-params:${Vers.junit}"
    val junit_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"
    val hamkrest = "com.natpryce:hamkrest:${Vers.hamkrest}"
    val hamcrest = "org.hamcrest:hamcrest-all:1.3"
}
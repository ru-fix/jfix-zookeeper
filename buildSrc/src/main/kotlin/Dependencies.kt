object Vers {
    //Plugins
    const val dokkav = "1.4.30"
    const val gradle_release_plugin = "1.4.3"
    const val asciidoctor = "1.6.0"
    const val asciidoctor_convert = "2.4.0"
    const val nexus_staging_plugin = "0.21.2"
    const val nexus_publish_plugin = "0.4.0"

    //Dependencies
    const val kotlin = "1.5.31"
    const val kotlin_gradle = "1.5.21"
    const val kotlin_coroutines = "1.3.5"
    const val jackson = "2.10.3"
    const val apache_curator = "5.0.0"
    const val junit_jupiter = "5.6.2"
    const val slf4j = "1.7.30"
    const val log4j = "2.13.2"
    const val jfix_stdlib = "3.0.0"
    const val awaitility = "4.0.3"
    const val netcrusher = "0.10"
}

object Libs {
    //Plugins
    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    const val asciidoctor = "org.asciidoctor:asciidoctor-gradle-plugin:${Vers.asciidoctor}"
    const val asciidoctor_convert = "org.asciidoctor.convert"
    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    //Dependencies
    const val gradle_kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin_gradle}"
    const val gradle_kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin_gradle}"

    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"
    const val kotlinx_coroutines_core = "org.jetbrains.kotlinx:kotlinx-coroutines-core:${Vers.kotlin_coroutines}"

    const val jfix_stdlib_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"
    const val jfix_stdlib_concurrency= "ru.fix:jfix-stdlib-concurrency:${Vers.jfix_stdlib}"

    const val slf4j_api = "org.slf4j:slf4j-api:${Vers.slf4j}"
    const val log4j_slf4j_impl = "org.apache.logging.log4j:log4j-slf4j-impl:${Vers.log4j}"
    const val log4j_kotlin = "org.apache.logging.log4j:log4j-api-kotlin:1.0.0"
    const val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"
    const val jackson_datatype_jsr310 = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${Vers.jackson}"
    const val jackson_kotlin_module = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson}"
    const val apache_curator_recipes = "org.apache.curator:curator-recipes:${Vers.apache_curator}"
    const val apache_curator_test = "org.apache.curator:curator-test:${Vers.apache_curator}"
    const val junit_jupiter_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit_jupiter}"
    const val junit_jupiter_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit_jupiter}"
    const val junit_jupiter_params = "org.junit.jupiter:junit-jupiter-params:${Vers.junit_jupiter}"
    const val awaitility = "org.awaitility:awaitility:${Vers.awaitility}"
    const val netcrusher = "com.github.netcrusherorg:netcrusher-core:${Vers.netcrusher}"
    const val kotest_assertions = "io.kotest:kotest-assertions-core-jvm:4.6.3"
    const val kotest_runner = "io.kotest:kotest-runner-junit5-jvm:4.6.3"
}


object Vers {
    //Plugins
    const val dokkav = "0.10.1"
    const val gradle_release_plugin = "1.3.17"
    const val asciidoctor = "1.6.0"
    const val asciidoctor_convert = "2.4.0"
    const val nexus_staging_plugin = "0.21.2"
    const val nexus_publish_plugin = "0.4.0"

    //Dependencies
    const val kotlin = "1.3.70"
    const val kotlin_coroutines = "1.3.5"
    const val lombok = "1.18.12"
    const val jackson = "2.10.3"
    const val jackson_kotlin = "2.10.3"
    const val apache_curator = "4.3.0"
    const val commons_io = "2.6"
    const val junit_jupiter = "5.6.2"
    const val log4j = "2.12.0"
    const val aggregating_profiler = "1.5.16"
    const val jfix_stdlib = "3.0.0"
    const val jfix_dynamic_property = "2.0.3"
}

object Libs {
    //Plugins
    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin =  "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    const val asciidoctor = "org.asciidoctor:asciidoctor-gradle-plugin:${Vers.asciidoctor}"
    const val asciidoctor_convert = "org.asciidoctor.convert"
    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    //Dependencies
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"
    const val kotlinx_coroutines_core = "org.jetbrains.kotlinx:kotlinx-coroutines-core:${Vers.kotlin_coroutines}"

    const val log4j = "org.apache.logging.log4j:log4j-api:${Vers.log4j}"
    const val log4j_core = "org.apache.logging.log4j:log4j-core:${Vers.log4j}"
    const val log4j_kotlin = "org.apache.logging.log4j:log4j-api-kotlin:1.0.0"
    const val slf4j_over_log4j = "org.apache.logging.log4j:log4j-slf4j-impl:${Vers.log4j}"

    const val aggregating_profiler = "ru.fix:aggregating-profiler:${Vers.aggregating_profiler}"
    const val jfix_concurrency = "ru.fix:jfix-stdlib-concurrency:${Vers.jfix_stdlib}"
    const val jfix_dynamic_property_api = "ru.fix:dynamic-property-api:${Vers.jfix_dynamic_property}"
    const val jfix_stdlib_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"

    const val lombok = "org.projectlombok:lombok:${Vers.lombok}"
    const val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"
    const val jackson_datatype_jsr310 = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${Vers.jackson}"
    const val jackson_kotlin_module = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson_kotlin}"
    const val apache_curator = "org.apache.curator:curator-recipes:${Vers.apache_curator}"
    const val apache_curator_discovery = "org.apache.curator:curator-x-discovery:${Vers.apache_curator}"
    const val apache_curator_test = "org.apache.curator:curator-test:${Vers.apache_curator}"
    const val junit_jupiter_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit_jupiter}"
    const val junit_jupiter_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit_jupiter}"
    const val commons_io = "commons-io:commons-io:${Vers.commons_io}"
}

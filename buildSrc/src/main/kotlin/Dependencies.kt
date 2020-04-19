
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
    const val jfix_stdlib = "3.0.0"
    const val lombok = "1.18.12"
    const val jackson = "2.10.3"
    const val jackson_kotlin = "2.10.3"
    const val apache_curator = "4.3.0"
    const val logback = "1.2.3"
    const val commons_io = "2.6"
    const val junit_jupiter = "5.6.2"
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

    const val jfix_stdlib_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"
    const val lombok = "org.projectlombok:lombok:${Vers.lombok}"
    const val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"
    const val jackson_datatype_jsr310 = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${Vers.jackson}"
    const val jackson_kotlin_module = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson_kotlin}"
    const val apache_curator = "org.apache.curator:curator-recipes:${Vers.apache_curator}"
    const val apache_curator_test = "org.apache.curator:curator-test:${Vers.apache_curator}"
    const val junit_jupiter_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit_jupiter}"
    const val junit_jupiter_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit_jupiter}"
    const val logback = "ch.qos.logback:logback-classic:${Vers.logback}"
    const val commons_io = "commons-io:commons-io:${Vers.commons_io}"
}
